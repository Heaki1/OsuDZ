     // Global state
        let currentData = {
            leaderboards: [],
            players: [],
            analytics: null,
            filters: {
                sort: 'score',
                mods: 'all',
                timeRange: 'all',
                search: ''
            }
        };

        let websocket = null;
        let apiBaseUrl = window.location.origin;

        // Initialize the application
        document.addEventListener('DOMContentLoaded', function() {
            initializeWebSocket();
            loadOverviewStats();
            loadLeaderboards();
            setupEventListeners();
            setupPeriodicUpdates();
        });

        // WebSocket connection
        function initializeWebSocket() {
            const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
            const wsUrl = `${protocol}//${window.location.host}/ws`;
            
            websocket = new WebSocket(wsUrl);
            
            websocket.onopen = function() {
                updateConnectionStatus('Connected', true);
                showNotification('Connected to live updates!', 'success');
            };
            
            websocket.onmessage = function(event) {
                const data = JSON.parse(event.data);
                handleWebSocketMessage(data);
            };
            
            websocket.onclose = function() {
                updateConnectionStatus('Disconnected', false);
                showNotification('Connection lost. Attempting to reconnect...', 'warning');
                setTimeout(initializeWebSocket, 5000);
            };
            
            websocket.onerror = function(error) {
                console.error('WebSocket error:', error);
                updateConnectionStatus('Error', false);
            };
        }

        function handleWebSocketMessage(data) {
            switch(data.type) {
                case 'new_scores':
                    showNotification(`New scores on ${data.beatmapTitle}!`, 'info');
                    if (getCurrentSection() === 'leaderboards') {
                        loadLeaderboards();
                    }
                    break;
                case 'new_player_discovered':
                    showNotification(`Welcome new player: ${data.player.username}!`, 'success');
                    break;
                case 'scan_complete':
                    showNotification('Leaderboard scan completed!', 'info');
                    loadOverviewStats();
                    break;
                case 'scan_progress':
                    updateScanProgress(data.progress);
                    break;
            }
        }

        function updateConnectionStatus(status, isConnected) {
            const statusElement = document.getElementById('connectionStatus');
            const statusDot = document.querySelector('.status-dot');
            
            statusElement.textContent = status;
            statusDot.style.background = isConnected ? '#00ff88' : '#ff4757';
        }

        // API calls
        async function apiCall(endpoint, options = {}) {
            try {
                const response = await fetch(`${apiBaseUrl}/api${endpoint}`, {
                    headers: {
                        'Content-Type': 'application/json',
                        ...options.headers
                    },
                    ...options
                });
                
                if (!response.ok) {
                    throw new Error(`HTTP ${response.status}: ${response.statusText}`);
                }
                
                return await response.json();
            } catch (error) {
                console.error('API call failed:', error);
                showNotification(`Failed to load data: ${error.message}`, 'error');
                return null;
            }
        }

        // Load overview statistics
        async function loadOverviewStats() {
            const data = await apiCall('/analytics/overview');
            
            if (data && data.success) {
                const stats = data.data.totalStats;
                document.getElementById('totalPlayers').textContent = formatNumber(stats.total_players);
                document.getElementById('totalScores').textContent = formatNumber(stats.total_scores);
                document.getElementById('totalBeatmaps').textContent = formatNumber(stats.total_beatmaps);
                document.getElementById('active24h').textContent = formatNumber(stats.active24h);
            }
        }

        // Load leaderboards data
        async function loadLeaderboards(offset = 0) {
            showLoading('leaderboardLoading', true);
            
            const params = new URLSearchParams({
                limit: 50,
                offset: offset,
                sort: currentData.filters.sort,
                order: 'DESC',
                timeRange: currentData.filters.timeRange !== 'all' ? currentData.filters.timeRange : '',
                mods: currentData.filters.mods !== 'all' ? currentData.filters.mods : '',
                player: currentData.filters.search
            });

            const data = await apiCall(`/leaderboards?${params}`);
            showLoading('leaderboardLoading', false);
            
            if (data && data.success) {
                currentData.leaderboards = data.data;
                renderLeaderboards(data.data);
            }
        }

        // Render leaderboards table
        function renderLeaderboards(scores) {
            const tbody = document.getElementById('leaderboardBody');
            tbody.innerHTML = '';
            
            if (scores.length === 0) {
                tbody.innerHTML = `
                    <tr>
                        <td colspan="8" style="text-align: center; padding: 2rem; color: var(--text-secondary);">
                            No scores found matching your criteria
                        </td>
                    </tr>
                `;
                return;
            }
            
            scores.forEach((score, index) => {
                const row = document.createElement('tr');
                row.innerHTML = `
                    <td class="rank-cell ${getRankClass(score.rank)}">#${score.rank}</td>
                    <td>
                        <div class="player-info">
                            <img class="player-avatar" src="https://a.ppy.sh/${score.player_id}" alt="${score.username}" 
                                 onerror="this.src='data:image/svg+xml,<svg xmlns=%22http://www.w3.org/2000/svg%22 viewBox=%220 0 100 100%22><circle cx=%2250%22 cy=%2250%22 r=%2250%22 fill=%22%23666%22/><text x=%2250%22 y=%2255%22 text-anchor=%22middle%22 fill=%22white%22 font-size=%2240%22>?</text></svg>'">
                            <div>
                                <div class="player-name" onclick="showPlayerModal('${score.username}')">${score.username}</div>
                            </div>
                        </div>
                    </td>
                    <td>
                        <div style="max-width: 250px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;" 
                             title="${score.beatmap_title}">
                            ${score.beatmap_title}
                        </div>
                        <div style="font-size: 0.8rem; color: var(--text-secondary);">
                            ⭐ ${score.difficulty_rating ? score.difficulty_rating.toFixed(2) : 'N/A'}
                        </div>
                    </td>
                    <td class="score-cell">${formatNumber(score.score)}</td>
                    <td>
                        <div>${score.accuracy_text}</div>
                        <div class="accuracy-bar">
                            <div class="accuracy-fill" style="width: ${(score.accuracy * 100)}%"></div>
                        </div>
                    </td>
                    <td>
                        <span class="mods-badge">${score.mods || 'None'}</span>
                    </td>
                    <td><strong>${score.pp ? score.pp.toFixed(0) + 'pp' : 'N/A'}</strong></td>
                    <td style="font-size: 0.9rem; color: var(--text-secondary);">
                        ${formatDate(score.play_date || score.last_updated)}
                    </td>
                `;
                tbody.appendChild(row);
            });
        }

        // Load top players
        async function loadPlayers() {
            showLoading('playersLoading', true);
            
            const data = await apiCall('/rankings?limit=20&sort=weighted_pp');
            showLoading('playersLoading', false);
            
            if (data && data.success) {
                currentData.players = data.data;
                renderPlayers(data.data);
            }
        }

        // Render players grid
        function renderPlayers(players) {
            const grid = document.getElementById('playersGrid');
            grid.innerHTML = '';
            
            if (players.length === 0) {
                grid.innerHTML = `
                    <div style="grid-column: 1/-1; text-align: center; padding: 2rem; color: var(--text-secondary);">
                        No players found
                    </div>
                `;
                return;
            }
            
            players.forEach((player, index) => {
                const card = document.createElement('div');
                card.className = 'player-card';
                card.onclick = () => showPlayerModal(player.username);
                
                card.innerHTML = `
                    <div class="player-header">
                        <img class="player-avatar-large" src="https://a.ppy.sh/${player.user_id}" alt="${player.username}"
                             onerror="this.src='data:image/svg+xml,<svg xmlns=%22http://www.w3.org/2000/svg%22 viewBox=%220 0 100 100%22><circle cx=%2250%22 cy=%2250%22 r=%2250%22 fill=%22%23666%22/><text x=%2250%22 y=%2255%22 text-anchor=%22middle%22 fill=%22white%22 font-size=%2240%22>?</text></svg>'">
                        <div>
                            <h3 style="margin: 0; color: var(--primary-color);">${player.username}</h3>
                            <p style="margin: 0; color: var(--text-secondary); font-size: 0.9rem;">
                                Rank #${player.rank || index + 1}
                            </p>
                        </div>
                        <div style="text-align: right;">
                            <div style="font-size: 1.2rem; font-weight: bold; color: var(--accent-color);">
                                ${player.weighted_pp ? player.weighted_pp.toFixed(0) + 'pp' : 'N/A'}
                            </div>
                        </div>
                    </div>
                    <div class="player-stats">
                        <div class="player-stat">
                            <span><i class="fas fa-trophy"></i> First Places</span>
                            <strong>${player.first_places || 0}</strong>
                        </div>
                        <div class="player-stat">
                            <span><i class="fas fa-target"></i> Avg Accuracy</span>
                            <strong>${player.accuracy_avg ? (player.accuracy_avg * 100).toFixed(2) + '%' : 'N/A'}</strong>
                        </div>
                        <div class="player-stat">
                            <span><i class="fas fa-gamepad"></i> Total Scores</span>
                            <strong>${player.total_scores || 0}</strong>
                        </div>
                        <div class="player-stat">
                            <span><i class="fas fa-star"></i> Top 10s</span>
                            <strong>${player.top_10_places || 0}</strong>
                        </div>
                    </div>
                `;
                
                grid.appendChild(card);
            });
        }

        // Load analytics data
        async function loadAnalytics() {
            const data = await apiCall('/analytics/overview');
            
            if (data && data.success) {
                currentData.analytics = data.data;
                renderAnalytics(data.data);
            }
        }

        // Render analytics
        function renderAnalytics(analytics) {
            const grid = document.getElementById('analyticsGrid');
            grid.innerHTML = '';
            
            // Mod usage statistics
            if (analytics.modUsage && analytics.modUsage.length > 0) {
                const modCard = document.createElement('div');
                modCard.className = 'stat-card';
                modCard.style.gridColumn = '1 / -1';
                
                let modChart = '';
                analytics.modUsage.slice(0, 5).forEach(mod => {
                    const percentage = (mod.usage_count / analytics.totalStats.total_scores * 100).toFixed(1);
                    modChart += `
                        <div style="display: flex; justify-content: space-between; align-items: center; margin: 0.5rem 0;">
                            <span class="mods-badge">${mod.mods}</span>
                            <span>${mod.usage_count} scores (${percentage}%)</span>
                        </div>
                    `;
                });
                
                modCard.innerHTML = `
                    <h4 style="color: var(--primary-color); margin-bottom: 1rem;">
                        <i class="fas fa-cogs"></i> Popular Mods
                    </h4>
                    ${modChart}
                `;
                
                grid.appendChild(modCard);
            }
            
            // Skill distribution (if available)
            if (analytics.skillDistribution && analytics.skillDistribution.length > 0) {
                const skillCard = document.createElement('div');
                skillCard.className = 'stat-card';
                skillCard.style.gridColumn = '1 / -1';
                
                let skillChart = '';
                analytics.skillDistribution.forEach(skill => {
                    const skillValue = parseFloat(skill.avg_value);
                    const percentage = (skillValue / 10 * 100);
                    skillChart += `
                        <div style="margin: 0.8rem 0;">
                            <div style="display: flex; justify-content: space-between; margin-bottom: 0.3rem;">
                                <span style="text-transform: capitalize;">${skill.skill_type}</span>
                                <span>${skillValue.toFixed(1)}/10</span>
                            </div>
                            <div style="width: 100%; height: 6px; background: rgba(255,255,255,0.1); border-radius: 3px; overflow: hidden;">
                                <div style="width: ${percentage}%; height: 100%; background: var(--gradient-2);"></div>
                            </div>
                        </div>
                    `;
                });
                
                skillCard.innerHTML = `
                    <h4 style="color: var(--primary-color); margin-bottom: 1rem;">
                        <i class="fas fa-chart-radar"></i> Average Skill Levels
                    </h4>
                    ${skillChart}
                `;
                
                grid.appendChild(skillCard);
            }
            
            // Top performers
            if (analytics.topPerformers && analytics.topPerformers.length > 0) {
                const topCard = document.createElement('div');
                topCard.className = 'stat-card';
                
                let topList = '';
                analytics.topPerformers.forEach((player, index) => {
                    topList += `
                        <div style="display: flex; align-items: center; gap: 10px; margin: 0.5rem 0; padding: 0.5rem; background: rgba(255,255,255,0.02); border-radius: 8px; cursor: pointer;" onclick="showPlayerModal('${player.username}')">
                            <span style="color: var(--accent-color); font-weight: bold; min-width: 30px;">#${index + 1}</span>
                            <img src="https://a.ppy.sh/${player.user_id}" alt="${player.username}" style="width: 24px; height: 24px; border-radius: 50%;"
                                 onerror="this.src='data:image/svg+xml,<svg xmlns=%22http://www.w3.org/2000/svg%22 viewBox=%220 0 100 100%22><circle cx=%2250%22 cy=%2250%22 r=%2250%22 fill=%22%23666%22/></svg>'">
                            <div style="flex: 1;">
                                <div style="font-weight: 600;">${player.username}</div>
                                <div style="font-size: 0.8rem; color: var(--text-secondary);">${player.weighted_pp ? player.weighted_pp.toFixed(0) + 'pp' : 'N/A'}</div>
                            </div>
                            <div style="color: var(--primary-color); font-weight: bold;">
                                👑 ${player.first_places || 0}
                            </div>
                        </div>
                    `;
                });
                
                topCard.innerHTML = `
                    <h4 style="color: var(--primary-color); margin-bottom: 1rem;">
                        <i class="fas fa-medal"></i> Top Performers
                    </h4>
                    ${topList}
                `;
                
                grid.appendChild(topCard);
            }
        }

        // Player modal
        async function showPlayerModal(username) {
            const modal = document.getElementById('playerModal');
            const title = document.getElementById('playerModalTitle');
            const content = document.getElementById('playerModalContent');
            
            title.textContent = `Loading ${username}...`;
            content.innerHTML = '<div class="loading"><div class="loading-spinner"></div></div>';
            modal.classList.add('active');
            
            const data = await apiCall(`/players/${encodeURIComponent(username)}`);
            
            if (data && data.success && data.data) {
                const player = data.data;
                title.textContent = player.username;
                
                content.innerHTML = `
                    <div style="display: flex; align-items: center; gap: 20px; margin-bottom: 2rem;">
                        <img src="https://a.ppy.sh/${player.user_id}" alt="${player.username}" 
                             style="width: 80px; height: 80px; border-radius: 50%; border: 3px solid var(--primary-color);"
                             onerror="this.src='data:image/svg+xml,<svg xmlns=%22http://www.w3.org/2000/svg%22 viewBox=%220 0 100 100%22><circle cx=%2250%22 cy=%2250%22 r=%2250%22 fill=%22%23666%22/><text x=%2250%22 y=%2255%22 text-anchor=%22middle%22 fill=%22white%22 font-size=%2240%22>?</text></svg>'">
                        <div>
                            <h2 style="margin: 0; color: var(--primary-color);">${player.username}</h2>
                            <p style="margin: 0.5rem 0; color: var(--text-secondary);">Country Rank #${player.countryRank || 'N/A'}</p>
                            <p style="margin: 0; font-size: 1.2rem; font-weight: bold; color: var(--accent-color);">
                                ${player.weighted_pp ? player.weighted_pp.toFixed(0) + 'pp' : 'N/A'}
                            </p>
                        </div>
                    </div>
                    
                    <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 1rem; margin-bottom: 2rem;">
                        <div class="player-stat">
                            <span><i class="fas fa-trophy"></i> First Places</span>
                            <strong>${player.first_places || 0}</strong>
                        </div>
                        <div class="player-stat">
                            <span><i class="fas fa-medal"></i> Top 10s</span>
                            <strong>${player.top_10_places || 0}</strong>
                        </div>
                        <div class="player-stat">
                            <span><i class="fas fa-gamepad"></i> Total Scores</span>
                            <strong>${player.total_scores || 0}</strong>
                        </div>
                        <div class="player-stat">
                            <span><i class="fas fa-target"></i> Avg Accuracy</span>
                            <strong>${player.accuracy_avg ? (player.accuracy_avg * 100).toFixed(2) + '%' : 'N/A'}</strong>
                        </div>
                        <div class="player-stat">
                            <span><i class="fas fa-star"></i> Best Score</span>
                            <strong>${formatNumber(player.best_score || 0)}</strong>
                        </div>
                        <div class="player-stat">
                            <span><i class="fas fa-chart-line"></i> Avg Rank</span>
                            <strong>#${player.avg_rank ? player.avg_rank.toFixed(1) : 'N/A'}</strong>
                        </div>
                    </div>
                    
                    ${player.recentScores && player.recentScores.length > 0 ? `
                        <h4 style="color: var(--primary-color); margin: 1.5rem 0 1rem 0;">
                            <i class="fas fa-clock"></i> Recent Scores
                        </h4>
                        <div style="max-height: 300px; overflow-y: auto;">
                            ${player.recentScores.slice(0, 5).map(score => `
                                <div style="display: flex; justify-content: space-between; align-items: center; padding: 0.8rem; background: rgba(255,255,255,0.02); border-radius: 8px; margin-bottom: 0.5rem;">
                                    <div style="flex: 1;">
                                        <div style="font-weight: 600; margin-bottom: 0.2rem;">${score.beatmap_title}</div>
                                        <div style="font-size: 0.8rem; color: var(--text-secondary);">
                                            Rank #${score.rank} • ${score.accuracy_text} • ${score.mods || 'None'}
                                        </div>
                                    </div>
                                    <div style="text-align: right;">
                                        <div style="font-weight: bold; color: var(--accent-color);">${formatNumber(score.score)}</div>
                                        <div style="font-size: 0.8rem; color: var(--text-secondary);">${score.pp ? score.pp.toFixed(0) + 'pp' : 'N/A'}</div>
                                    </div>
                                </div>
                            `).join('')}
                        </div>
                    ` : ''}
                    
                    ${player.achievements && player.achievements.length > 0 ? `
                        <h4 style="color: var(--primary-color); margin: 1.5rem 0 1rem 0;">
                            <i class="fas fa-award"></i> Achievements (${player.achievements.length})
                        </h4>
                        <div style="display: grid; grid-template-columns: repeat(auto-fill, minmax(200px, 1fr)); gap: 0.8rem;">
                            ${player.achievements.slice(0, 6).map(achievement => `
                                <div style="display: flex; align-items: center; gap: 10px; padding: 0.6rem; background: rgba(255,255,255,0.02); border-radius: 8px; border-left: 3px solid var(--accent-color);">
                                    <span style="font-size: 1.2rem;">${achievement.icon}</span>
                                    <div>
                                        <div style="font-weight: 600; font-size: 0.9rem;">${achievement.name}</div>
                                        <div style="font-size: 0.8rem; color: var(--text-secondary);">${achievement.points}pts</div>
                                    </div>
                                </div>
                            `).join('')}
                        </div>
                    ` : ''}
                `;
            } else {
                title.textContent = 'Error';
                content.innerHTML = `
                    <div style="text-align: center; padding: 2rem; color: var(--text-secondary);">
                        <i class="fas fa-exclamation-triangle" style="font-size: 3rem; margin-bottom: 1rem; color: #ff4757;"></i>
                        <p>Unable to load player data for "${username}"</p>
                        <p style="font-size: 0.9rem; margin-top: 0.5rem;">The player might not be tracked yet or there was a connection error.</p>
                    </div>
                `;
            }
        }

        function closeModal(modalId) {
            document.getElementById(modalId).classList.remove('active');
        }

        // Event listeners
        function setupEventListeners() {
            // Navigation
            document.querySelectorAll('.nav-btn').forEach(btn => {
                btn.addEventListener('click', function() {
                    const section = this.dataset.section;
                    switchSection(section);
                });
            });
            
            // Search and filters
            document.getElementById('searchInput').addEventListener('input', debounce(function() {
                currentData.filters.search = this.value;
                if (getCurrentSection() === 'leaderboards') {
                    loadLeaderboards();
                }
            }, 300));
            
            document.getElementById('sortFilter').addEventListener('change', function() {
                currentData.filters.sort = this.value;
                loadLeaderboards();
            });
            
            document.getElementById('modsFilter').addEventListener('change', function() {
                currentData.filters.mods = this.value;
                loadLeaderboards();
            });
            
            document.getElementById('timeFilter').addEventListener('change', function() {
                currentData.filters.timeRange = this.value;
                loadLeaderboards();
            });
            
            // Close modals on outside click
            document.addEventListener('click', function(e) {
                if (e.target.classList.contains('modal')) {
                    e.target.classList.remove('active');
                }
            });
            
            // Keyboard shortcuts
            document.addEventListener('keydown', function(e) {
                if (e.key === 'Escape') {
                    document.querySelectorAll('.modal.active').forEach(modal => {
                        modal.classList.remove('active');
                    });
                }
                
                if (e.ctrlKey && e.key === 'k') {
                    e.preventDefault();
                    document.getElementById('searchInput').focus();
                }
            });
        }

        // Section switching
        function switchSection(sectionName) {
            // Update navigation
            document.querySelectorAll('.nav-btn').forEach(btn => {
                btn.classList.remove('active');
            });
            document.querySelector(`[data-section="${sectionName}"]`).classList.add('active');
            
            // Hide all sections
            document.querySelectorAll('.section[id$="Section"]').forEach(section => {
                section.style.display = 'none';
            });
            
            // Show selected section
            const targetSection = document.getElementById(`${sectionName}Section`);
            if (targetSection) {
                targetSection.style.display = 'block';
            }
            
            // Load data for the section
            switch(sectionName) {
                case 'leaderboards':
                    if (currentData.leaderboards.length === 0) {
                        loadLeaderboards();
                    }
                    break;
                case 'players':
                    if (currentData.players.length === 0) {
                        loadPlayers();
                    }
                    break;
                case 'analytics':
                    if (!currentData.analytics) {
                        loadAnalytics();
                    }
                    break;
            }
        }

        function getCurrentSection() {
            return document.querySelector('.nav-btn.active')?.dataset.section || 'leaderboards';
        }

        // Utility functions
        function formatNumber(num) {
            if (num === null || num === undefined) return '0';
            return parseInt(num).toLocaleString();
        }

        function formatDate(timestamp) {
            if (!timestamp) return 'N/A';
            const date = new Date(parseInt(timestamp));
            const now = new Date();
            const diff = now - date;
            
            const minutes = Math.floor(diff / (1000 * 60));
            const hours = Math.floor(diff / (1000 * 60 * 60));
            const days = Math.floor(diff / (1000 * 60 * 60 * 24));
            
            if (minutes < 60) return `${minutes}m ago`;
            if (hours < 24) return `${hours}h ago`;
            if (days < 7) return `${days}d ago`;
            
            return date.toLocaleDateString();
        }

        function getRankClass(rank) {
            if (rank === 1) return 'rank-1';
            if (rank === 2) return 'rank-2';
            if (rank === 3) return 'rank-3';
            return '';
        }

        function showLoading(elementId, show) {
            const element = document.getElementById(elementId);
            if (element) {
                element.style.display = show ? 'flex' : 'none';
            }
        }

        function showNotification(message, type = 'info') {
            const notification = document.createElement('div');
            notification.className = 'notification';
            notification.innerHTML = `
                <div style="display: flex; align-items: center; gap: 10px;">
                    <i class="fas fa-${getNotificationIcon(type)}"></i>
                    <span>${message}</span>
                </div>
            `;
            
            document.body.appendChild(notification);
            
            setTimeout(() => notification.classList.add('show'), 100);
            setTimeout(() => {
                notification.classList.remove('show');
                setTimeout(() => document.body.removeChild(notification), 300);
            }, 4000);
        }

        function getNotificationIcon(type) {
            const icons = {
                success: 'check-circle',
                error: 'exclamation-circle',
                warning: 'exclamation-triangle',
                info: 'info-circle'
            };
            return icons[type] || icons.info;
        }

        function debounce(func, wait) {
            let timeout;
            return function executedFunction(...args) {
                const later = () => {
                    clearTimeout(timeout);
                    func(...args);
                };
                clearTimeout(timeout);
                timeout = setTimeout(later, wait);
            };
        }

        function updateScanProgress(progress) {
            // You can implement a progress bar here if needed
            console.log(`Scan progress: ${progress.percentage}%`);
        }

        // Periodic updates
        function setupPeriodicUpdates() {
            // Refresh overview stats every 5 minutes
            setInterval(loadOverviewStats, 5 * 60 * 1000);
            
            // Refresh current section data every 2 minutes
            setInterval(() => {
                const currentSection = getCurrentSection();
                switch(currentSection) {
                    case 'leaderboards':
                        loadLeaderboards();
                        break;
                    case 'players':
                        loadPlayers();
                        break;
                    case 'analytics':
                        loadAnalytics();
                        break;
                }
            }, 2 * 60 * 1000);
        }

        // Handle page visibility changes
        document.addEventListener('visibilitychange', function() {
            if (!document.hidden) {
                // Page became visible, refresh data
                loadOverviewStats();
                const currentSection = getCurrentSection();
                if (currentSection === 'leaderboards') {
                    loadLeaderboards();
                }
            }
        });

        // Service worker registration (optional, for offline functionality)
        if ('serviceWorker' in navigator) {
            window.addEventListener('load', function() {
                // You can implement a service worker here for offline functionality
            });
        }