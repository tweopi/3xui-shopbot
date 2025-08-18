document.addEventListener('DOMContentLoaded', function () {
    function initializePasswordToggles() {
        const togglePasswordButtons = document.querySelectorAll('.toggle-password');
        togglePasswordButtons.forEach(button => {
            button.addEventListener('click', function () {
                const parent = this.closest('.form-group') || this.closest('.password-wrapper');
                if (!parent) return;

                const passwordInput = parent.querySelector('input');
                if (!passwordInput) return;

                if (passwordInput.type === 'password') {
                    passwordInput.type = 'text';
                    this.textContent = '🙈';
                } else {
                    passwordInput.type = 'password';
                    this.textContent = '👁️';
                }
            });
        });
    }

    function setupBotControlForms() {
        const startForm = document.querySelector('form[action*="start-bot"]');
        const stopForm = document.querySelector('form[action*="stop-bot"]');

        if (startForm) {
            startForm.addEventListener('submit', function () {
                const button = startForm.querySelector('button[type="submit"]');
                if (button) {
                    button.disabled = true;
                    button.textContent = 'Запускаем...';
                }
            });
        }

        if (stopForm) {
            stopForm.addEventListener('submit', function () {
                const button = stopForm.querySelector('button[type="submit"]');
                if (button) {
                    button.disabled = true;
                    button.textContent = 'Останавливаем...';
                }
            });
        }
    }

    function setupConfirmationForms() {
        const confirmationForms = document.querySelectorAll('form[data-confirm]');
        confirmationForms.forEach(form => {
            form.addEventListener('submit', function (event) {
                const message = form.getAttribute('data-confirm');
                if (!confirm(message)) {
                    event.preventDefault();
                }
            });
        });
    }

    function initializeDashboardCharts() {
        const usersChartCanvas = document.getElementById('newUsersChart');
        if (!usersChartCanvas || typeof CHART_DATA === 'undefined') {
            return;
        }

        function prepareChartData(data, label, color) {
            const labels = [];
            const values = [];
            const today = new Date();

            for (let i = 29; i >= 0; i--) {
                const date = new Date(today);
                date.setDate(today.getDate() - i);
                const dateString = date.toISOString().split('T')[0];
                const formattedDate = `${date.getDate().toString().padStart(2, '0')}.${(date.getMonth() + 1).toString().padStart(2, '0')}`;
                labels.push(formattedDate);
                values.push(data[dateString] || 0);
            }

            return {
                labels: labels,
                datasets: [
                    {
                        label: label,
                        data: values,
                        borderColor: color,
                        backgroundColor: color + '33',
                        borderWidth: 2,
                        fill: true,
                        tension: 0.3,
                    },
                ],
            };
        }

        function updateChartFontsAndLabels(chart) {
            const isMobile = window.innerWidth <= 768;
            const isVerySmall = window.innerWidth <= 470;
            chart.options.scales.x.ticks.font.size = isMobile ? 10 : 12;
            chart.options.scales.y.ticks.font.size = isMobile ? 10 : 12;
            chart.options.plugins.legend.labels.font.size = isMobile ? 12 : 14;
            chart.options.scales.x.ticks.maxTicksLimit = isMobile ? 8 : 15;
            // Скрываем метки и легенду при ширине <= 470px
            chart.options.scales.x.ticks.display = !isVerySmall;
            chart.options.scales.y.ticks.display = !isVerySmall;
            chart.options.plugins.legend.display = !isVerySmall;
            chart.update();
        }

        const usersCtx = usersChartCanvas.getContext('2d');
        const usersChartData = prepareChartData(
            CHART_DATA.users,
            'Новых пользователей в день',
            '#007bff'
        );
        const usersChart = new Chart(usersCtx, {
            type: 'line',
            data: usersChartData,
            options: {
                scales: {
                    y: {
                        beginAtZero: true,
                        ticks: {
                            precision: 0,
                            font: {
                                size: window.innerWidth <= 768 ? 10 : 12
                            },
                            display: window.innerWidth > 470
                        }
                    },
                    x: {
                        ticks: {
                            font: {
                                size: window.innerWidth <= 768 ? 10 : 12
                            },
                            maxTicksLimit: window.innerWidth <= 768 ? 8 : 15,
                            maxRotation: 45,
                            minRotation: 45,
                            display: window.innerWidth > 470
                        }
                    }
                },
                responsive: true,
                maintainAspectRatio: false,
                layout: {
                    autoPadding: true,
                    padding: 0
                },
                plugins: {
                    legend: {
                        labels: {
                            font: {
                                size: window.innerWidth <= 768 ? 12 : 14
                            },
                            display: window.innerWidth > 470
                        }
                    }
                }
            }
        });

        const keysChartCanvas = document.getElementById('newKeysChart');
        if (!keysChartCanvas) return;

        const keysCtx = keysChartCanvas.getContext('2d');
        const keysChartData = prepareChartData(
            CHART_DATA.keys,
            'Новых ключей в день',
            '#28a745'
        );
        const keysChart = new Chart(keysCtx, {
            type: 'line',
            data: keysChartData,
            options: {
                scales: {
                    y: {
                        beginAtZero: true,
                        ticks: {
                            precision: 0,
                            font: {
                                size: window.innerWidth <= 768 ? 10 : 12
                            },
                            display: window.innerWidth > 470
                        }
                    },
                    x: {
                        ticks: {
                            font: {
                                size: window.innerWidth <= 768 ? 10 : 12
                            },
                            maxTicksLimit: window.innerWidth <= 768 ? 8 : 15,
                            maxRotation: 45,
                            minRotation: 45,
                            display: window.innerWidth > 470
                        }
                    }
                },
                responsive: true,
                maintainAspectRatio: false,
                layout: {
                    autoPadding: true,
                    padding: 0
                },
                plugins: {
                    legend: {
                        labels: {
                            font: {
                                size: window.innerWidth <= 768 ? 12 : 14
                            },
                            display: window.innerWidth > 470
                        }
                    }
                }
            }
        });

        window.addEventListener('resize', () => {
            updateChartFontsAndLabels(usersChart);
            updateChartFontsAndLabels(keysChart);
        });
    }

    function initializeTicketAutoRefresh() {
        const root = document.getElementById('ticket-root');
        if (!root) return;

        const ticketId = root.getAttribute('data-ticket-id');
        const chatBox = document.getElementById('chat-box');
        const statusEl = document.getElementById('ticket-status');
        if (!ticketId || !chatBox || !statusEl) return;

        let lastKey = '';
        let lastCount = 0;

        function buildMessageNode(m) {
            const wrap = document.createElement('div');
            wrap.className = 'chat-message ' + (m.sender === 'admin' ? 'from-admin' : 'from-user');

            const meta = document.createElement('div');
            meta.className = 'meta';
            const sender = document.createElement('span');
            sender.className = 'sender';
            sender.textContent = m.sender === 'admin' ? 'Админ' : 'Пользователь';
            const time = document.createElement('span');
            time.className = 'time';
            time.textContent = m.created_at || '';
            meta.appendChild(sender);
            meta.appendChild(time);

            const content = document.createElement('div');
            content.className = 'content';
            content.textContent = m.content || '';

            wrap.appendChild(meta);
            wrap.appendChild(content);
            return wrap;
        }

        function updateStatus(status) {
            if (status === 'open') {
                statusEl.innerHTML = '<span class="indicator indicator-lg indicator--green pulse"></span><span class="badge badge-green">Открыт</span>';
                const textarea = document.getElementById('reply-text');
                const replyBtn = document.getElementById('reply-btn');
                if (textarea) textarea.disabled = false;
                if (replyBtn) replyBtn.disabled = false;
                const toggleBtn = document.getElementById('toggle-status-btn');
                if (toggleBtn) { toggleBtn.textContent = 'Закрыть'; toggleBtn.value = 'close'; toggleBtn.className = 'button button-danger'; }
            } else {
                statusEl.innerHTML = '<span class="indicator indicator-lg indicator--gray"></span><span class="badge">Закрыт</span>';
                const textarea = document.getElementById('reply-text');
                const replyBtn = document.getElementById('reply-btn');
                if (textarea) textarea.disabled = true;
                if (replyBtn) replyBtn.disabled = true;
                const toggleBtn = document.getElementById('toggle-status-btn');
                if (toggleBtn) { toggleBtn.textContent = 'Открыть'; toggleBtn.value = 'open'; toggleBtn.className = 'button button-start'; }
            }
        }

        async function fetchAndRender() {
            try {
                const resp = await fetch(`/support/${ticketId}/messages.json`, { headers: { 'Accept': 'application/json' } });
                if (!resp.ok) return;
                const data = await resp.json();
                const items = Array.isArray(data.messages) ? data.messages : [];
                const key = JSON.stringify({ len: items.length, last: items[items.length - 1] || null, status: data.status });
                if (key === lastKey) return;

                // Remember scroll position to decide autoscroll
                const nearBottom = (chatBox.scrollHeight - chatBox.scrollTop - chatBox.clientHeight) < 60;

                // Render messages safely
                chatBox.innerHTML = '';
                if (items.length === 0) {
                    const p = document.createElement('p');
                    p.className = 'no-messages';
                    p.textContent = 'Сообщений пока нет.';
                    chatBox.appendChild(p);
                } else {
                    for (let i = 0; i < items.length; i++) {
                        const node = buildMessageNode(items[i]);
                        if (i >= lastCount) {
                            node.classList.add('flash');
                            setTimeout(() => node.classList.remove('flash'), 1800);
                        }
                        chatBox.appendChild(node);
                    }
                }

                updateStatus(data.status);

                if (nearBottom) {
                    chatBox.scrollTop = chatBox.scrollHeight;
                }

                lastKey = key;
                lastCount = items.length;
            } catch (e) {
                // no-op: silent network errors
            }
        }

        // Initial fetch and periodic polling
        fetchAndRender();
        const interval = setInterval(fetchAndRender, 2500);

        // Clear interval when navigating away
        window.addEventListener('beforeunload', () => clearInterval(interval));
    }

    function initializeGlobalAutoRefresh() {
        const page = document.body.getAttribute('data-page') || '';
        // Исключаем страницу настроек и страницу тикета (у тикета свой пуллинг)
        if (page === 'settings_page' || page === 'support_ticket_page') return;

        let typing = false;
        document.addEventListener('keydown', (e) => {
            const t = e.target;
            if (t && (t.tagName === 'INPUT' || t.tagName === 'TEXTAREA' || t.isContentEditable)) {
                typing = true;
                setTimeout(() => typing = false, 1500);
            }
        });

        function tryReload() {
            if (document.hidden) return; // вкладка не активна
            if (typing) return;          // пользователь печатает
            if (performance.now() < 3000) return; // не перезагружать сразу после загрузки
            location.reload();
        }

        const intervalMs = page === 'dashboard_page' ? 8000 : 10000;
        const interval = setInterval(tryReload, intervalMs);
        window.addEventListener('beforeunload', () => clearInterval(interval));
    }

    initializePasswordToggles();
    setupBotControlForms();
    setupConfirmationForms();
    initializeDashboardCharts();
    initializeTicketAutoRefresh();
    initializeGlobalAutoRefresh();
});