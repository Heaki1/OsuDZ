const WebSocket = require('ws');

class WebSocketManager {
  constructor() {
    this.wss = null;
    this.connections = new Set();
  }

  initialize(server) {
    this.wss = new WebSocket.Server({ 
      server, 
      path: '/ws',
      clientTracking: true
    });

    this.wss.on('connection', (ws, req) => {
      console.log('🔌 New WebSocket connection from', req.connection.remoteAddress);
      
      this.connections.add(ws);
      
      // Send welcome message
      ws.send(JSON.stringify({ 
        type: 'connected', 
        timestamp: Date.now(),
        message: 'Connected to Algeria osu! Leaderboards'
      }));

      // Handle incoming messages
      ws.on('message', (data) => {
        try {
          const message = JSON.parse(data.toString());
          this.handleMessage(ws, message);
        } catch (err) {
          console.error('❌ Invalid WebSocket message:', err.message);
        }
      });

      // Handle connection close
      ws.on('close', (code, reason) => {
        console.log('🔌 WebSocket connection closed:', code, reason.toString());
        this.connections.delete(ws);
      });

      // Handle errors
      ws.on('error', (error) => {
        console.error('🔌 WebSocket error:', error.message);
        this.connections.delete(ws);
      });

      // Send periodic ping to keep connection alive
      const pingInterval = setInterval(() => {
        if (ws.readyState === WebSocket.OPEN) {
          ws.ping();
        } else {
          clearInterval(pingInterval);
        }
      }, 30000); // 30 seconds

      ws.on('close', () => clearInterval(pingInterval));
    });

    // Cleanup dead connections
    setInterval(() => {
      this.cleanupConnections();
    }, 60000); // 1 minute

    console.log('✅ WebSocket server initialized');
  }

  handleMessage(ws, message) {
    switch (message.type) {
      case 'ping':
        ws.send(JSON.stringify({ type: 'pong', timestamp: Date.now() }));
        break;
      
      case 'subscribe':
        // Handle subscription to specific events
        ws.subscriptions = ws.subscriptions || new Set();
        if (message.events) {
          message.events.forEach(event => ws.subscriptions.add(event));
        }
        ws.send(JSON.stringify({ 
          type: 'subscribed', 
          events: Array.from(ws.subscriptions),
          timestamp: Date.now() 
        }));
        break;
      
      case 'unsubscribe':
        if (ws.subscriptions && message.events) {
          message.events.forEach(event => ws.subscriptions.delete(event));
        }
        break;
      
      default:
        console.warn('❓ Unknown WebSocket message type:', message.type);
    }
  }

  broadcast(data) {
    if (!this.wss) return;

    const message = JSON.stringify(data);
    let sentCount = 0;
    let errorCount = 0;

    this.connections.forEach(client => {
      if (client.readyState === WebSocket.OPEN) {
        // Check subscription filter
        if (client.subscriptions && data.type) {
          if (!client.subscriptions.has(data.type) && !client.subscriptions.has('*')) {
            return; // Skip if client isn't subscribed to this event type
          }
        }

        try {
          client.send(message);
          sentCount++;
        } catch (err) {
          console.error('❌ Failed to send WebSocket message:', err.message);
          errorCount++;
          this.connections.delete(client);
        }
      } else {
        this.connections.delete(client);
      }
    });

    if (sentCount > 0) {
      console.log(`📡 Broadcasted '${data.type}' to ${sentCount} clients${errorCount > 0 ? ` (${errorCount} errors)` : ''}`);
    }

    return { sent: sentCount, errors: errorCount };
  }

  cleanupConnections() {
    const before = this.connections.size;
    
    this.connections.forEach(client => {
      if (client.readyState !== WebSocket.OPEN) {
        this.connections.delete(client);
      }
    });

    const cleaned = before - this.connections.size;
    if (cleaned > 0) {
      console.log(`🧹 Cleaned up ${cleaned} dead WebSocket connections`);
    }
  }

  getStats() {
    return {
      totalConnections: this.connections.size,
      activeConnections: Array.from(this.connections).filter(
        client => client.readyState === WebSocket.OPEN
      ).length
    };
  }

  close() {
    if (this.wss) {
      this.connections.forEach(client => {
        try {
          client.close(1000, 'Server shutting down');
        } catch (err) {
          // Ignore errors during shutdown
        }
      });
      
      return new Promise(resolve => {
        this.wss.close(resolve);
      });
    }
    return Promise.resolve();
  }
}

// Create singleton instance
const wsManager = new WebSocketManager();

// Export both the manager and a convenience function
module.exports = {
  WebSocketManager,
  wsManager,
  broadcastToClients: (data) => wsManager.broadcast(data)
};