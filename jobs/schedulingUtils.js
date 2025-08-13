// jobs/schedulingUtils.js
const { broadcastToClients } = require('../middleware/websocket');

// Job status tracking
const jobStatus = new Map();

// Helper to prevent overlapping jobs with enhanced tracking
function scheduleInterval(fn, intervalMs, label, options = {}) {
  const {
    maxRetries = 3,
    retryDelay = 5000,
    skipOnError = false,
    enableBroadcast = true
  } = options;

  let running = false;
  let lastRun = null;
  let lastError = null;
  let consecutiveFailures = 0;

  // Initialize job status
  jobStatus.set(label, {
    label,
    intervalMs,
    running: false,
    lastRun: null,
    lastError: null,
    consecutiveFailures: 0,
    totalRuns: 0,
    totalFailures: 0,
    nextRun: Date.now() + intervalMs
  });

  const runJob = async (retryCount = 0) => {
    if (running) {
      console.warn(`⚠️ Skipped scheduled task '${label}' because previous run is still running`);
      return;
    }

    const status = jobStatus.get(label);
    running = true;
    status.running = true;
    status.totalRuns++;

    if (enableBroadcast) {
      broadcastToClients({
        type: 'job_started',
        job: label,
        timestamp: Date.now()
      });
    }

    try {
      console.log(`🔄 Starting scheduled task: ${label}`);
      const startTime = Date.now();
      
      const result = await fn();
      
      const duration = Date.now() - startTime;
      lastRun = Date.now();
      lastError = null;
      consecutiveFailures = 0;

      // Update status
      status.lastRun = lastRun;
      status.lastError = null;
      status.consecutiveFailures = 0;
      status.nextRun = lastRun + intervalMs;

      console.log(`✅ Scheduled task '${label}' completed successfully in ${duration}ms`);
      
      if (enableBroadcast) {
        broadcastToClients({
          type: 'job_completed',
          job: label,
          duration,
          result,
          timestamp: Date.now()
        });
      }

    } catch (err) {
      lastError = err.message;
      consecutiveFailures++;
      
      // Update status
      status.lastError = lastError;
      status.consecutiveFailures = consecutiveFailures;
      status.totalFailures++;

      console.error(`❌ Scheduled task '${label}' failed (attempt ${retryCount + 1}):`, err.message);
      
      if (enableBroadcast) {
        broadcastToClients({
          type: 'job_failed',
          job: label,
          error: err.message,
          retryCount,
          timestamp: Date.now()
        });
      }

      // Retry logic
      if (retryCount < maxRetries && !skipOnError) {
        console.log(`🔄 Retrying '${label}' in ${retryDelay}ms (attempt ${retryCount + 1}/${maxRetries})`);
        setTimeout(() => runJob(retryCount + 1), retryDelay);
        return;
      }

      // If all retries failed or skipOnError is true
      if (consecutiveFailures >= 5) {
        console.error(`🚨 Task '${label}' has failed ${consecutiveFailures} times consecutively. Consider manual intervention.`);
      }

    } finally {
      running = false;
      status.running = false;
    }
  };

  const intervalId = setInterval(runJob, intervalMs);

  // Store interval ID for cleanup
  status.intervalId = intervalId;

  return {
    stop: () => {
      clearInterval(intervalId);
      jobStatus.delete(label);
      console.log(`🛑 Stopped scheduled task: ${label}`);
    },
    runNow: () => runJob(),
    getStatus: () => ({ ...jobStatus.get(label) })
  };
}

// Schedule a one-time job with delay
function scheduleOnce(fn, delay, label) {
  console.log(`⏰ Scheduling one-time task '${label}' to run in ${delay}ms`);
  
  return setTimeout(async () => {
    try {
      console.log(`🚀 Running one-time task: ${label}`);
      const result = await fn();
      console.log(`✅ One-time task '${label}' completed`);
      return result;
    } catch (err) {
      console.error(`❌ One-time task '${label}' failed:`, err.message);
      throw err;
    }
  }, delay);
}

// Get status of all jobs
function getAllJobStatus() {
  const status = {};
  for (const [label, jobInfo] of jobStatus.entries()) {
    status[label] = { ...jobInfo };
    delete status[label].intervalId; // Don't expose internal interval ID
  }
  return status;
}

// Stop all jobs (for graceful shutdown)
function stopAllJobs() {
  console.log('🛑 Stopping all scheduled jobs...');
  
  for (const [label, jobInfo] of jobStatus.entries()) {
    if (jobInfo.intervalId) {
      clearInterval(jobInfo.intervalId);
      console.log(`  ✅ Stopped: ${label}`);
    }
  }
  
  jobStatus.clear();
  console.log('✅ All scheduled jobs stopped');
}

// Graceful delay helper
function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Parse cron-like expressions to milliseconds
function parseCronToMs(cronExpr) {
  const patterns = {
    '*/5 * * * *': 5 * 60 * 1000,        // Every 5 minutes
    '0 * * * *': 60 * 60 * 1000,         // Every hour
    '0 */2 * * *': 2 * 60 * 60 * 1000,   // Every 2 hours
    '0 */6 * * *': 6 * 60 * 60 * 1000,   // Every 6 hours
    '0 */12 * * *': 12 * 60 * 60 * 1000, // Every 12 hours
    '0 0 * * *': 24 * 60 * 60 * 1000,    // Daily
    '0 0 * * 0': 7 * 24 * 60 * 60 * 1000 // Weekly
  };
  
  return patterns[cronExpr] || null;
}

module.exports = {
  scheduleInterval,
  scheduleOnce,
  getAllJobStatus,
  stopAllJobs,
  delay,
  parseCronToMs
};