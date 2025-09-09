document.addEventListener('DOMContentLoaded', function () {
    // CSRF helper (meta -> token)
    function getCsrfToken(){
        const meta = document.querySelector('meta[name="csrf-token"]');
        return meta ? meta.getAttribute('content') : '';
    }
    // Programmatic toast API
    window.showToast = function(category, message, delay){
        try{
            const cont = document.getElementById('toast-container');
            if (!cont) return;
            const el = document.createElement('div');
            const cat = (category === 'danger' ? 'danger' : (category === 'success' ? 'success' : (category === 'warning' ? 'warning' : 'secondary')));
            el.className = 'toast fade align-items-center text-bg-' + cat;
            el.setAttribute('role','alert'); el.setAttribute('aria-live','assertive'); el.setAttribute('aria-atomic','true');
            el.innerHTML = '<div class="d-flex"><div class="toast-body">'+ (message||'') +'</div><button type="button" class="btn-close btn-close-white me-2 m-auto" data-bs-dismiss="toast" aria-label="Close"></button></div>';
            cont.appendChild(el);
            new bootstrap.Toast(el, { delay: Math.max(2000, delay||4000), autohide: true }).show();
        }catch(_){ }
    }
    // Global partial refresh by container id
    window.refreshContainerById = async function(id){
        const node = document.getElementById(id);
        if (!node) return;
        const url = node.getAttribute('data-fetch-url');
        if (!url) return;
        try {
            const resp = await fetch(url, { headers: { 'Accept': 'text/html' }, cache: 'no-store', credentials: 'same-origin' });
            if (resp.redirected) { window.location.href = resp.url; return; }
            if (resp.status === 401 || resp.status === 403) { window.location.href = '/login'; return; }
            if (!resp.ok) return;
            const html = await resp.text();
            if (html && html !== node.innerHTML) {
                // lock height to avoid layout shift
                const prevH = node.offsetHeight;
                if (prevH > 0) node.style.minHeight = prevH + 'px';
                node.classList.add('is-swapping');
                node.innerHTML = html;
                try {
                    node.classList.add('flash');
                    setTimeout(()=> node.classList.remove('flash'), 600);
                } catch(_){ }
                try { initTooltipsWithin(node); } catch(_){ }
                // Re-bind confirmation/AJAX handlers for newly injected forms
                try { setupConfirmationForms(node); } catch(_){ }
                // unlock after transition
                setTimeout(()=>{ node.style.minHeight = ''; node.classList.remove('is-swapping'); }, 260);
            }
        } catch(_){ }
    }
    // Init tooltips helpers
    function initTooltipsWithin(root){
        if (!window.bootstrap) return;
        const scope = root || document;
        // уничтожаем старые тултипы, если есть data-bs-toggle
        scope.querySelectorAll('[data-bs-toggle="tooltip"]').forEach(el=>{
            try { bootstrap.Tooltip.getInstance(el)?.dispose(); } catch(_){ }
        });
        const targets = scope.querySelectorAll('[data-bs-toggle="tooltip"], .btn[title], a.btn[title]');
        targets.forEach(el=>{
            try { new bootstrap.Tooltip(el, { container: 'body' }); } catch(_){ }
        });
    }
    // Attach CSRF token to all POST forms
    function initializeCsrfForForms() {
        const meta = document.querySelector('meta[name="csrf-token"]');
        const token = meta ? meta.getAttribute('content') : null;
        if (!token) return;

        document.querySelectorAll('form').forEach(form => {
            const method = (form.getAttribute('method') || '').toLowerCase();
            if (method !== 'post') return;
            form.addEventListener('submit', function () {
                if (form.querySelector('input[name="csrf_token"]')) return;
                const input = document.createElement('input');
                input.type = 'hidden';
                input.name = 'csrf_token';
                input.value = token;
                form.appendChild(input);
            });
        });
    }
    // Theme toggle: persists selection and updates <html data-bs-theme>
    function initializeThemeToggle() {
        const THEME_KEY = 'ui_theme';
        const root = document.documentElement; // <html>
        const btn = document.getElementById('theme-toggle');
        const label = btn ? btn.querySelector('.theme-label') : null;

        function applyTheme(theme) {
            const next = (theme === 'light' || theme === 'dark') ? theme : 'dark';
            root.setAttribute('data-bs-theme', next);
            try { localStorage.setItem(THEME_KEY, next); } catch (_) {}
            if (label) label.textContent = next === 'dark' ? 'Тёмная' : 'Светлая';
        }

        // Get saved or default theme
        let saved = 'dark';
        try { saved = localStorage.getItem(THEME_KEY) || 'dark'; } catch (_) {}
        applyTheme(saved);

        if (btn) {
            btn.addEventListener('click', function () {
                const current = root.getAttribute('data-bs-theme') || 'dark';
                const next = current === 'dark' ? 'light' : 'dark';
                applyTheme(next);
            });
        }
    }

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
                    this.textContent = '';
                } else {
                    passwordInput.type = 'password';
                    this.textContent = '';
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
                    button.textContent = '...';
                }
            });
        }

        if (stopForm) {
            stopForm.addEventListener('submit', function () {
                const button = stopForm.querySelector('button[type="submit"]');
                if (button) {
                    button.disabled = true;
                    button.textContent = '...';
                }
            });
        }
    }

    function setupConfirmationForms(root) {
        const scope = root || document;
        const forms = scope.querySelectorAll('form[data-confirm]');
        forms.forEach(form => {
            form.addEventListener('submit', async function (event) {
                const message = form.getAttribute('data-confirm');
                if (!confirm(message)) {
                    event.preventDefault();
                    return;
                }
                // AJAX delete
                if (form.getAttribute('data-ajax') === 'delete') {
                    event.preventDefault();
                    try {
                        const fd = new FormData(form);
                        // ensure csrf present
                        if (!fd.get('csrf_token')){
                            const t = getCsrfToken();
                            if (t) fd.append('csrf_token', t);
                        }
                        const resp = await fetch(form.action, { method: 'POST', body: fd, credentials: 'same-origin' });
                        if (resp.ok) {
                            const action = form.getAttribute('data-action');
                            const msg = action === 'revoke-keys' ? 'Ключи отозваны' : 'Удалено';
                            try { window.showToast('success', msg); } catch(_){ }
                            const targetId = form.getAttribute('data-refresh-target');
                            if (targetId) { try { await window.refreshContainerById(targetId); } catch(_){ } }
                        } else {
                            const action = form.getAttribute('data-action');
                            const msg = action === 'revoke-keys' ? 'Не удалось отозвать ключи' : 'Не удалось удалить';
                            try { window.showToast('danger', msg); } catch(_){ }
                        }
                    } catch(_){ try { window.showToast('danger', 'Ошибка удаления'); } catch(__){} }
                }
            });
        });
    }

    // Center all modals by default (unless already centered)
    document.querySelectorAll('.modal .modal-dialog').forEach(dlg => {
        if (!dlg.classList.contains('modal-dialog-centered')) {
            dlg.classList.add('modal-dialog-centered');
        }
    });

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
            // 
            chart.options.scales.x.ticks.display = !isVerySmall;
            chart.options.scales.y.ticks.display = !isVerySmall;
            chart.options.plugins.legend.display = !isVerySmall;
            chart.update();
        }

        const usersCtx = usersChartCanvas.getContext('2d');
        const usersChartData = prepareChartData(
            CHART_DATA.users,
            'Новые пользователи',
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
                            display: true
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
                            display: true
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
                        display: true,
                        labels: {
                            font: {
                                size: window.innerWidth <= 768 ? 12 : 14
                            }
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
            'Новые ключи',
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

        // --- Auto refresh charts data without page reload ---
        async function refreshCharts(){
            try{
                const resp = await fetch('/dashboard/charts.json', { headers: { 'Accept': 'application/json' }, credentials: 'same-origin', cache: 'no-store' });
                if (resp.redirected) { window.location.href = resp.url; return; }
                if (resp.status === 401 || resp.status === 403) { window.location.href = '/login'; return; }
                if (!resp.ok) return;
                const fresh = await resp.json();
                if (!fresh) return;
                const newUsers = prepareChartData(fresh.users, 'Новые пользователи', '#007bff');
                const newKeys = prepareChartData(fresh.keys, 'Новые ключи', '#28a745');
                usersChart.data.labels = newUsers.labels;
                usersChart.data.datasets[0].data = newUsers.datasets[0].data;
                keysChart.data.labels = newKeys.labels;
                keysChart.data.datasets[0].data = newKeys.datasets[0].data;
                usersChart.update('none');
                keysChart.update('none');
            }catch(_){/* noop */}
        }
        setInterval(refreshCharts, 10000);
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
                statusEl.innerHTML = '<span class="status-dot status-dot-animated bg-green"></span><span class="badge bg-green ms-1">Открыт</span>';
                const textarea = document.getElementById('reply-text');
                const replyBtn = document.getElementById('reply-btn');
                if (textarea) textarea.disabled = false;
                if (replyBtn) replyBtn.disabled = false;
                const toggleBtn = document.getElementById('toggle-status-btn');
                if (toggleBtn) { toggleBtn.textContent = 'Закрыть'; toggleBtn.value = 'close'; toggleBtn.className = 'btn btn-danger'; }
            } else {
                statusEl.innerHTML = '<span class="status-dot"></span><span class="badge ms-1">Закрыт</span>';
                const textarea = document.getElementById('reply-text');
                const replyBtn = document.getElementById('reply-btn');
                if (textarea) textarea.disabled = true;
                if (replyBtn) replyBtn.disabled = true;
                const toggleBtn = document.getElementById('toggle-status-btn');
                if (toggleBtn) { toggleBtn.textContent = 'Открыть'; toggleBtn.value = 'open'; toggleBtn.className = 'btn btn-success'; }
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

                const nearBottom = (chatBox.scrollHeight - chatBox.scrollTop - chatBox.clientHeight) < 60;

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
                // silent
            }
        }

        fetchAndRender();
        const interval = setInterval(fetchAndRender, 2500);
        window.addEventListener('beforeunload', () => clearInterval(interval));
    }

    // Глобальная авто-перезагрузка отключена по требованию: оставляем только точечные авто-обновления без reload
    function initializeGlobalAutoRefresh() {
        /* disabled */
    }

    // Черновая схема выборочного автообновления блоков по data-fetch-url (под будущие включения)
    function initializeSoftAutoUpdate() {
        const nodes = Array.from(document.querySelectorAll('[data-fetch-url]'));
        if (!nodes.length) return;
        nodes.forEach(node => {
            const url = node.getAttribute('data-fetch-url');
            const interval = Number(node.getAttribute('data-fetch-interval')||'8000');
            if (!url) return;
            let timer = null;
            async function tick(){
                try{
                    const resp = await fetch(url, { headers: { 'Accept': 'text/html' }, cache: 'no-store', credentials: 'same-origin' });
                    if (resp.redirected) { window.location.href = resp.url; return; }
                    if (resp.status === 401 || resp.status === 403) { window.location.href = '/login'; return; }
                    if (!resp.ok) return;
                    const html = await resp.text();
                    if (html && html !== node.innerHTML) {
                        const prevH = node.offsetHeight;
                        if (prevH > 0) node.style.minHeight = prevH + 'px';
                        node.classList.add('is-swapping');
                        node.innerHTML = html;
                        try {
                            node.classList.add('flash');
                            setTimeout(()=> node.classList.remove('flash'), 600);
                        } catch(_){ }
                        // re-init tooltips and confirmation handlers for new content
                        try { initTooltipsWithin(node); } catch(_){ }
                        try { setupConfirmationForms(node); } catch(_){ }
                        setTimeout(()=>{ node.style.minHeight = ''; node.classList.remove('is-swapping'); }, 260);
                    }
                }catch(_){/* noop */}
            }
            tick();
            timer = setInterval(tick, Math.max(4000, interval));
            node.addEventListener('soft-update-stop', ()=>{ if (timer){ clearInterval(timer); timer=null; } });
            window.addEventListener('beforeunload', ()=>{ if (timer){ clearInterval(timer); timer=null; } });
        });
    }

    // Settings tabs: show/hide sections by hash and set active nav link
    function initializeSettingsTabs() {
        const nav = document.querySelector('.nav.nav-pills');
        const container = document.querySelector('.settings-container');
        if (!nav || !container) return; // not on settings page

        const links = Array.from(nav.querySelectorAll('a.nav-link'));
        // Собираем все секции автоматически
        const sections = Array.from(document.querySelectorAll('.settings-section'));
        const rightCol = document.querySelector('.settings-column-right');

        function show(targetHash) {
            const hash = (targetHash && targetHash.startsWith('#')) ? targetHash : '#panel';
            // зафиксируем высоту правой колонки на время анимации, чтобы не было "рывков"
            let currentVisible = document.querySelector('.settings-column-right .settings-section:not(.is-hidden)');
            const currentHeight = currentVisible ? currentVisible.offsetHeight : 0;
            const targetEl = document.querySelector(hash);
            const targetHeight = targetEl ? targetEl.offsetHeight : 0;
            if (rightCol) {
                const h = Math.max(currentHeight, targetHeight);
                if (h > 0) rightCol.style.minHeight = h + 'px';
            }
            sections.forEach(sec => {
                if ('#' + sec.id === hash) {
                    sec.classList.remove('is-hidden');
                } else {
                    sec.classList.add('is-hidden');
                }
            });
            links.forEach(a => {
                if (a.getAttribute('href') === hash) a.classList.add('active');
                else a.classList.remove('active');
            });
            // Если не нашли секцию по hash — показать первую существующую
            const anyVisible = sections.some(sec => !sec.classList.contains('is-hidden'));
            if (!anyVisible && sections.length) {
                sections[0].classList.remove('is-hidden');
            }
            // снять фиксацию высоты после завершения анимации скрытия/показа
            if (rightCol) setTimeout(() => { rightCol.style.minHeight = ''; }, 260);

            // Единый макет для вкладки Хосты: скрываем правую колонку, растягиваем левую
            const leftCol = document.querySelector('.settings-column-left');
            if (hash === '#hosts') {
                if (rightCol) rightCol.style.display = 'none';
                if (leftCol) {
                    leftCol.style.flex = '1 1 100%';
                    leftCol.style.minWidth = '0';
                }
            } else {
                if (rightCol) rightCol.style.display = '';
                if (leftCol) {
                    leftCol.style.flex = '';
                    leftCol.style.minWidth = '';
                }
            }
        }

        // Click navigation без скачка страницы: используем replaceState и синхронизируем ?tab
        links.forEach(a => {
            a.addEventListener('click', (e) => {
                e.preventDefault();
                const href = a.getAttribute('href');
                if (!href) return;
                const y = window.scrollY;
                show(href);
                // Обновим адресную строку (и hash, и ?tab), не триггеря прокрутку
                const tabName = href.startsWith('#') ? href.slice(1) : href;
                try { history.replaceState(null, '', `?tab=${encodeURIComponent(tabName)}${href}`); } catch(_) {}
                // восстановим исходную позицию
                window.scrollTo(0, y);
            });
        });

        // Обработка внешнего изменения hash (ручной ввод) без скачка
        window.addEventListener('hashchange', () => {
            const y = window.scrollY;
            show(location.hash);
            window.scrollTo(0, y);
        });

        // Initial state (без скачка): поддержка ?tab=...
        const params = new URLSearchParams(window.location.search);
        const tabParam = params.get('tab');
        const initialHash = tabParam ? `#${tabParam}` : (location.hash || '#panel');
        show(initialHash);
        // Синхронизируем URL, чтобы и hash, и ?tab были установлены
        try {
            const tabName = initialHash.startsWith('#') ? initialHash.slice(1) : initialHash;
            history.replaceState(null, '', `?tab=${encodeURIComponent(tabName)}${initialHash}`);
        } catch(_) {}
    }

    // Initialize modules once DOM is ready
    initTooltipsWithin(document);
    initializePasswordToggles();
    setupBotControlForms();
    setupConfirmationForms();
    initializeDashboardCharts();
    initializeTicketAutoRefresh();
    // Автоперезагрузка выключена: initializeGlobalAutoRefresh();
    initializeSoftAutoUpdate();
    initializeSettingsTabs();
    initializeThemeToggle();
    initializeCsrfForForms();

    // Inline edit rows (URL/Имя хоста)
    document.querySelectorAll('[data-edit-row]').forEach(row => {
        const input = row.querySelector('[data-edit-target]');
        const btnEdit = row.querySelector('[data-action="edit"]');
        const btnSave = row.querySelector('[data-action="save"]');
        const btnCancel = row.querySelector('[data-action="cancel"]');
        if (!input || !btnEdit || !btnSave || !btnCancel) return;

        const orig = { value: input.value };
        function setMode(editing) {
            if (editing) {
                input.readOnly = false;
                input.classList.add('is-editing');
                btnEdit.classList.add('d-none');
                btnSave.classList.remove('d-none');
                btnCancel.classList.remove('d-none');
                input.focus();
                try { input.setSelectionRange(input.value.length, input.value.length); } catch(_) {}
            } else {
                input.readOnly = true;
                input.classList.remove('is-editing');
                btnEdit.classList.remove('d-none');
                btnSave.classList.add('d-none');
                btnCancel.classList.add('d-none');
            }
        }

        btnEdit.addEventListener('click', () => setMode(true));
        btnCancel.addEventListener('click', () => { input.value = orig.value; setMode(false); });
        row.addEventListener('submit', () => { orig.value = input.value; setMode(false); });
    });
});