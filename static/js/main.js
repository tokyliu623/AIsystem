document.addEventListener('DOMContentLoaded', function() {
    const sessionId = generateUUID();

    /* ========== Toast System ========== */
    function showToast(message, type = 'info', duration = 3000) {
        const container = document.getElementById('toast-container');
        if (!container) return;
        const toast = document.createElement('div');
        toast.className = `toast ${type}`;
        toast.innerHTML = `<span>${message}</span>`;
        container.appendChild(toast);
        if (duration > 0) {
            setTimeout(() => {
                toast.classList.add('removing');
                setTimeout(() => toast.remove(), 250);
            }, duration);
        }
    }

    /* ========== Confirm Dialog ========== */
    function showConfirm(title, message) {
        return new Promise(resolve => {
            const overlay = document.getElementById('confirm-overlay');
            const titleEl = document.getElementById('confirm-title');
            const msgEl = document.getElementById('confirm-message');
            const okBtn = document.getElementById('confirm-ok');
            const cancelBtn = document.getElementById('confirm-cancel');
            if (!overlay) { resolve(true); return; }
            titleEl.textContent = title;
            msgEl.textContent = message;
            overlay.classList.remove('hidden');
            function cleanup() {
                overlay.classList.add('hidden');
                okBtn.removeEventListener('click', onOk);
                cancelBtn.removeEventListener('click', onCancel);
            }
            function onOk() { cleanup(); resolve(true); }
            function onCancel() { cleanup(); resolve(false); }
            okBtn.addEventListener('click', onOk);
            cancelBtn.addEventListener('click', onCancel);
        });
    }

    /* ========== Step Indicator ========== */
    function updateSteps(type, stepName) {
        const tab = document.getElementById(`${type}-tab`);
        if (!tab) return;
        const steps = tab.querySelectorAll('.steps-indicator .step');
        const order = ['api-key', 'upload', 'audit', 'result'];
        const idx = order.indexOf(stepName);
        steps.forEach((s, i) => {
            s.classList.remove('active', 'done');
            if (i < idx) s.classList.add('done');
            else if (i === idx) s.classList.add('active');
        });
    }
    function initSteps(type) {
        const tab = document.getElementById(`${type}-tab`);
        if (!tab) return;
        const container = tab.querySelector('.steps-indicator');
        if (!container || container.querySelector('.step')) return;
        const steps = [
            { id: 'api-key', label: '输入密钥' },
            { id: 'upload', label: '上传文件' },
            { id: 'audit', label: '开始巡检' },
            { id: 'result', label: '查看结果' }
        ];
        container.innerHTML = steps.map((s, i) => {
            const active = i === 0 ? ' active' : '';
            const conn = i < steps.length - 1 ? '<div class="step-connector"></div>' : '';
            return `<div class="step${active}" data-step="${s.id}"><span class="step-number">${i + 1}</span><span class="step-label">${s.label}</span></div>${conn}`;
        }).join('');
    }

    /* ========== Navigation ========== */
    const pageTitleMap = {
        comment: '评论巡检', cover: '封面巡检', push: 'Push巡检',
        brand: '品牌守护', news: '资讯巡检', history: '历史记录', settings: '系统配置'
    };
    function switchTab(tabName) {
        document.querySelectorAll('.nav-item').forEach(n => n.classList.remove('active'));
        const navItem = document.querySelector(`.nav-item[data-tab="${tabName}"]`);
        if (navItem) navItem.classList.add('active');
        document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
        const tab = document.getElementById(`${tabName}-tab`);
        if (tab) { tab.classList.add('active'); initSteps(tabName); }
        const titleEl = document.getElementById('current-page-title');
        if (titleEl) titleEl.textContent = pageTitleMap[tabName] || '';
        if (tabName === 'history') initializeHistoryPage();
        if (tabName === 'settings') loadSettings();
        if (tabName === 'comment' || tabName === 'push' || tabName === 'cover')
            initializeMultiApiKeyInputs(tabName);
        document.getElementById('sidebar')?.classList.remove('open');
        document.getElementById('sidebar-overlay')?.classList.remove('show');
    }
    document.querySelectorAll('.nav-item').forEach(n => n.addEventListener('click', e => {
        e.preventDefault();
        switchTab(n.dataset.tab);
    }));

    /* Mobile menu */
    const menuToggle = document.getElementById('menu-toggle');
    const sidebarOverlay = document.getElementById('sidebar-overlay');
    menuToggle?.addEventListener('click', () => {
        document.getElementById('sidebar')?.classList.toggle('open');
        sidebarOverlay?.classList.toggle('show');
    });
    sidebarOverlay?.addEventListener('click', () => {
        document.getElementById('sidebar')?.classList.remove('open');
        sidebarOverlay?.classList.remove('show');
    });

    /* ========== Utility ========== */
    function generateUUID() {
        return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
            const r = Math.random() * 16 | 0;
            return (c === 'x' ? r : (r & 0x3 | 0x8)).toString(16);
        });
    }
    function formatSize(bytes) {
        if (bytes < 1024) return bytes + ' B';
        if (bytes < 1048576) return (bytes / 1024).toFixed(1) + ' KB';
        return (bytes / 1048576).toFixed(1) + ' MB';
    }
    function formatDuration(seconds) {
        if (seconds < 60) return `~${Math.ceil(seconds)}秒`;
        if (seconds < 3600) return `~${Math.ceil(seconds / 60)}分钟`;
        return `~${Math.ceil(seconds / 3600)}小时`;
    }

    /* ========== Multi API Key (comment/push/cover) ========== */
    function initializeMultiApiKeyInputs(type) {
        const container = document.getElementById(`${type}-api-keys-container`);
        if (!container) return;
        if (container.querySelector('.api-key-input-group')) return;
        resetApiKeys(type);
    }
    function resetApiKeys(type) {
        const container = document.getElementById(`${type}-api-keys-container`);
        if (!container) return;
        container.innerHTML = `<div class="api-key-input-group"><input type="text" class="api-key-input" placeholder="请输入API密钥 1" data-index="1"><button type="button" class="remove-api-key-btn" style="display:none"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg></button></div>`;
        document.getElementById(`${type}-add-api-key`).onclick = () => addApiKeyInput(type);
        container.addEventListener('input', e => {
            if (e.target.classList.contains('api-key-input')) { validateApiKeys(type); checkStartBtn(type); }
        });
        container.addEventListener('click', e => {
            if (e.target.classList.contains('remove-api-key-btn') || e.target.closest('.remove-api-key-btn')) {
                removeApiKeyInput(type, e.target.closest('.api-key-input-group'));
            }
        });
    }
    function addApiKeyInput(type) {
        const container = document.getElementById(`${type}-api-keys-container`);
        const groups = container.querySelectorAll('.api-key-input-group');
        if (groups.length >= 10) { showToast('最多只能添加10个API密钥', 'warning'); return; }
        const idx = groups.length + 1;
        const g = document.createElement('div');
        g.className = 'api-key-input-group';
        g.innerHTML = `<input type="text" class="api-key-input" placeholder="请输入API密钥 ${idx}" data-index="${idx}"><button type="button" class="remove-api-key-btn"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg></button>`;
        container.appendChild(g);
        if (groups.length > 0) {
            const firstRemove = groups[0].querySelector('.remove-api-key-btn');
            if (firstRemove) firstRemove.style.display = 'flex';
        }
        validateApiKeys(type);
        checkStartBtn(type);
    }
    function removeApiKeyInput(type, group) {
        const container = document.getElementById(`${type}-api-keys-container`);
        if (!container || container.querySelectorAll('.api-key-input-group').length <= 1) {
            showToast('至少需要一个API密钥', 'warning');
            return;
        }
        group.remove();
        const gs = container.querySelectorAll('.api-key-input-group');
        gs.forEach((g, i) => {
            const inp = g.querySelector('.api-key-input');
            inp.dataset.index = i + 1;
            inp.placeholder = `请输入API密钥 ${i + 1}`;
            const rb = g.querySelector('.remove-api-key-btn');
            if (rb) rb.style.display = i === 0 ? 'none' : 'flex';
        });
        validateApiKeys(type);
        checkStartBtn(type);
    }
    function validateApiKeys(type) {
        const keys = getApiKeys(type);
        const el = document.getElementById(`${type}-api-status`);
        if (!el) return keys.length > 0;
        if (keys.length === 0) { el.textContent = '请至少输入一个API密钥'; el.className = 'api-status invalid'; }
        else { el.textContent = `已输入 ${keys.length}/10 个`; el.className = 'api-status valid'; }
        return keys.length > 0;
    }
    function getApiKeys(type) {
        const container = document.getElementById(`${type}-api-keys-container`);
        if (!container) return [];
        return Array.from(container.querySelectorAll('.api-key-input')).map(i => i.value.trim()).filter(v => v);
    }
    function checkStartBtn(type) {
        const btn = document.getElementById(`${type}-start-btn`);
        if (!btn) return;
        const hasFile = document.getElementById(`${type}-file`)?.files?.length > 0;
        btn.disabled = !(validateApiKeys(type) && hasFile);
    }
    function runMultiAgentTask(type) {
        const keys = getApiKeys(type);
        if (!keys.length) { showToast('请至少输入一个API密钥', 'error'); return; }
        fetch('/run', {
            method: 'POST', headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ audit_type: type, api_keys: keys, session_id: sessionId })
        }).then(r => r.json()).then(d => {
            if (d.error) { showToast(d.error, 'error'); }
            else { showToast('任务已启动', 'success'); updateUIForProcessing(type); }
        }).catch(() => showToast('启动任务时发生错误', 'error'));
    }

    /* ========== File Upload & Drag Drop ========== */
    function initUpload(type) {
        const zone = document.getElementById(`${type}-upload-zone`);
        const fileInput = document.getElementById(`${type}-file`);
        const fileInfo = document.getElementById(`${type}-file-info`);
        const fileNameEl = document.getElementById(`${type}-file-name`);
        const fileSizeEl = document.getElementById(`${type}-file-size`);
        const removeBtn = document.getElementById(`${type}-file-remove`);

        zone?.addEventListener('dragover', e => { e.preventDefault(); zone.classList.add('dragover'); });
        zone?.addEventListener('dragleave', () => zone.classList.remove('dragover'));
        zone?.addEventListener('drop', e => {
            e.preventDefault();
            zone.classList.remove('dragover');
            if (e.dataTransfer.files.length > 0) {
                fileInput.files = e.dataTransfer.files;
                handleFileSelect(type, e.dataTransfer.files[0], fileInput, fileInfo, fileNameEl, fileSizeEl);
            }
        });
        zone?.addEventListener('click', e => {
            if (!e.target.closest('.file-input-hidden')) fileInput.click();
        });
        fileInput?.addEventListener('change', () => {
            if (fileInput.files.length > 0) {
                handleFileSelect(type, fileInput.files[0], fileInput, fileInfo, fileNameEl, fileSizeEl);
            }
        });
        removeBtn?.addEventListener('click', () => {
            fileInput.value = '';
            fileInfo.classList.add('hidden');
            zone.style.display = '';
            const startBtn = document.getElementById(`${type}-start-btn`);
            if (startBtn) startBtn.disabled = true;
            updateSteps(type, 'api-key');
        });
    }

    function handleFileSelect(type, file, fileInput, fileInfo, fileNameEl, fileSizeEl) {
        fileNameEl.textContent = file.name;
        fileSizeEl.textContent = formatSize(file.size);
        fileInfo.classList.remove('hidden');
        if (fileInfo.previousElementSibling) {
            fileInfo.previousElementSibling.style.display = 'none';
        }
        updateSteps(type, 'upload');
        uploadFile(type, file);
        if (type === 'news') checkNewsKeys();
        else if (type === 'comment' || type === 'push' || type === 'cover') checkStartBtn(type);
        else {
            const key = document.getElementById(`${type}-api-key`);
            const btn = document.getElementById(`${type}-start-btn`);
            if (btn) btn.disabled = !(key && key.value.trim());
        }
    }

    function uploadFile(type, file) {
        const fd = new FormData();
        fd.append('file', file);
        fd.append('audit_type', type);
        fd.append('session_id', sessionId);
        fetch('/upload', { method: 'POST', body: fd })
            .then(r => r.json())
            .then(d => {
                if (d.error) { showToast(d.error, 'error'); }
                else { showToast('文件上传成功', 'success'); }
            })
            .catch(() => showToast('文件上传失败', 'error'));
    }

    /* ========== Check News Keys ========== */
    function checkNewsKeys() {
        const k1 = document.getElementById('news-api-key-agent1')?.value || '';
        const k2 = document.getElementById('news-api-key-agent2')?.value || '';
        const k3 = document.getElementById('news-api-key-agent3')?.value || '';
        const hasFile = document.getElementById('news-file')?.files?.length > 0;
        const btn = document.getElementById('news-start-btn');
        if (btn) btn.disabled = !(k1.trim() && k2.trim() && k3.trim() && hasFile);
    }
    ['agent1', 'agent2', 'agent3'].forEach(a => {
        document.getElementById(`news-api-key-${a}`)?.addEventListener('input', () => {
            checkNewsKeys();
            const keys = [
                document.getElementById('news-api-key-agent1')?.value,
                document.getElementById('news-api-key-agent2')?.value,
                document.getElementById('news-api-key-agent3')?.value
            ];
            const filledKeys = keys.filter(k => k.trim());
            const statusEl = document.getElementById('news-api-status');
            if (statusEl) {
                if (filledKeys.length === 0) { statusEl.textContent = '等待输入'; statusEl.className = 'api-status'; }
                else if (filledKeys.length < 3) { statusEl.textContent = `已输入 ${filledKeys.length}/3 个`; statusEl.className = 'api-status invalid'; }
                else { statusEl.textContent = '3个密钥已就绪'; statusEl.className = 'api-status valid'; }
            }
        });
    });

    /* ========== Audit Types Init ========== */
    const auditTypes = ['comment', 'cover', 'push', 'brand', 'news'];
    auditTypes.forEach(type => {
        initSteps(type);
        initUpload(type);
        setInterval(() => checkStatus(type), 2000);
    });

    /* ========== Brand API Key Init ========== */
    document.getElementById('brand-api-key')?.addEventListener('input', function() {
        const hasFile = document.getElementById('brand-file')?.files?.length > 0;
        const btn = document.getElementById('brand-start-btn');
        if (btn) btn.disabled = !(this.value.trim() && hasFile);
        const statusEl = document.getElementById('brand-api-status');
        if (statusEl) {
            if (!this.value.trim()) { statusEl.textContent = '等待输入'; statusEl.className = 'api-status'; }
            else { statusEl.textContent = '密钥已就绪'; statusEl.className = 'api-status valid'; }
        }
        if (this.value.trim()) { updateSteps('brand', 'upload'); }
        else { updateSteps('brand', 'api-key'); }
    });

    /* ========== Status Message ========== */
    function showStatusMessage(type, msg, status = 'info') {
        const el = document.getElementById(`${type}-status-message`);
        if (!el) return;
        el.className = 'status-banner';
        if (status === 'error') el.classList.add('error');
        else if (status === 'success') el.classList.add('success');
        else if (status === 'warning') el.classList.add('warning');
        const textSpan = el.querySelector('.status-text');
        if (textSpan) textSpan.textContent = msg;
        else el.textContent = msg;
    }

    /* ========== Progress Section ========== */
    function showProgressSection(type, show) {
        const sec = document.getElementById(`${type}-progress-section`);
        if (sec) sec.style.display = show ? 'block' : 'none';
    }

    /* ========== Run / Control ========== */
    function setupButtons(type) {
        const startBtn = document.getElementById(`${type}-start-btn`);
        startBtn?.addEventListener('click', () => {
            updateSteps(type, 'audit');
            if (type === 'news') {
                const k1 = document.getElementById('news-api-key-agent1')?.value.trim();
                const k2 = document.getElementById('news-api-key-agent2')?.value.trim();
                const k3 = document.getElementById('news-api-key-agent3')?.value.trim();
                if (!k1 || !k2 || !k3) { showToast('三个Agent的API密钥都不能为空', 'error'); return; }
                runTask(type, `${k1}|||${k2}|||${k3}`);
            } else if (type === 'comment' || type === 'push' || type === 'cover') {
                runMultiAgentTask(type);
            } else {
                const apiKey = document.getElementById(`${type}-api-key`)?.value.trim();
                if (!apiKey) { showToast('API密钥不能为空', 'error'); return; }
                runTask(type, apiKey);
            }
        });
        document.getElementById(`${type}-pause-btn`)?.addEventListener('click', () => controlTask(type, 'pause'));
        document.getElementById(`${type}-resume-btn`)?.addEventListener('click', () => controlTask(type, 'resume'));
        document.getElementById(`${type}-finish-btn`)?.addEventListener('click', () => controlTask(type, 'finish'));
        document.getElementById(`${type}-end-btn`)?.addEventListener('click', async () => {
            const ok = await showConfirm('结束任务', '确定要结束当前任务吗？未保存的进度将丢失。');
            if (!ok) return;
            controlTask(type, 'end');
        });
        document.getElementById(`${type}-download-btn`)?.addEventListener('click', () => {
            window.location.href = `/download/${type}?session_id=${sessionId}`;
            showToast('开始下载...', 'success');
        });
    }
    auditTypes.forEach(setupButtons);
    setupButtons('comment');

    function runTask(type, apiKey) {
        fetch('/run', {
            method: 'POST', headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ audit_type: type, api_key: apiKey, session_id: sessionId })
        }).then(r => r.json()).then(d => {
            if (d.error) { showToast(d.error, 'error'); }
            else { showToast('任务已启动', 'success'); updateUIForProcessing(type); }
        }).catch(() => showToast('启动任务时发生错误', 'error'));
    }

    function controlTask(type, action) {
        fetch('/control', {
            method: 'POST', headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ audit_type: type, action, session_id: sessionId })
        }).then(r => r.json()).then(d => {
            if (d.error) { showToast(d.error, 'error'); }
            else {
                showToast(d.message, 'success');
                if (action === 'pause') updateUIForPaused(type);
                else if (action === 'resume') updateUIForProcessing(type);
                else if (action === 'finish') updateUIForDone(type);
                else if (action === 'end') updateUIForReset(type);
            }
        }).catch(() => showToast(`${action}任务时发生错误`, 'error'));
    }

    /* ========== Check Status ========== */
    const startTimes = {};
    const processedCounts = {};
    function checkStatus(type) {
        fetch(`/status/${type}?session_id=${sessionId}`)
            .then(r => r.json()).then(d => {
                if (d.error) return;
                const bar = document.getElementById(`${type}-progress-bar-fill`);
                if (bar) bar.style.width = `${d.progress}%`;
                const pct = document.getElementById(`${type}-progress-pct`);
                if (pct) pct.textContent = `${d.progress}%`;
                const txt = document.getElementById(`${type}-progress-text`);
                if (txt) txt.textContent = `${d.processed} / ${d.total} 条`;

                // Speed
                if (d.status === 'processing' && d.processed > 0) {
                    if (!startTimes[type]) { startTimes[type] = performance.now(); processedCounts[type] = d.processed; }
                    const elapsed = (performance.now() - startTimes[type]) / 1000;
                    if (elapsed > 0 && d.processed > (processedCounts[type] || 0)) {
                        processedCounts[type] = d.processed;
                        const rate = d.processed / elapsed;
                        const speedEl = document.getElementById(`${type}-speed`);
                        if (speedEl) speedEl.textContent = `${rate.toFixed(1)}条/秒`;
                        const remaining = d.total - d.processed;
                        const eta = remaining / rate;
                        const etaEl = document.getElementById(`${type}-eta`);
                        if (etaEl) etaEl.textContent = `约${formatDuration(eta)}`;
                    }
                }

                if (d.message) showStatusMessage(type, d.message, d.status === 'error' ? 'error' : d.status === 'warning' ? 'warning' : 'info');

                if (d.status === 'processing') updateUIForProcessing(type);
                else if (d.status === 'done') updateUIForDone(type);
                else if (d.status === 'error') updateUIForError(type);

                // Subtasks
                if (d.subtasks) renderSubtasks(type, d.subtasks);
            }).catch(() => {});
    }

    function renderSubtasks(type, subtasks) {
        const container = document.getElementById(`${type}-subtasks`);
        if (!container) return;
        if (Object.keys(subtasks).length === 0) { container.classList.add('hidden'); return; }
        container.classList.remove('hidden');
        container.innerHTML = Object.entries(subtasks).map(([k, v]) => {
            const st = v.status === 'completed' ? 'success' : v.status === 'error' ? 'error' : '';
            return `<div class="subtask-item ${st}"><span class="subtask-label">子任务 ${k}</span><div class="subtask-bar"><div class="subtask-fill" style="width:${v.progress}%"></div></div><span>${v.progress}%</span></div>`;
        }).join('');
    }

    /* ========== UI States ========== */
    function disableAllKeys(type) {
        if (type === 'comment' || type === 'push' || type === 'cover') {
            document.querySelectorAll(`#${type}-api-keys-container .api-key-input`).forEach(i => i.disabled = true);
            document.getElementById(`${type}-add-api-key`) && (document.getElementById(`${type}-add-api-key`).disabled = true);
        } else if (type === 'news') {
            ['agent1', 'agent2', 'agent3'].forEach(a => {
                const inp = document.getElementById(`news-api-key-${a}`);
                if (inp) inp.disabled = true;
            });
        } else {
            const inp = document.getElementById(`${type}-api-key`);
            if (inp) inp.disabled = true;
        }
    }
    function enableAllKeys(type) {
        if (type === 'comment' || type === 'push' || type === 'cover') {
            document.querySelectorAll(`#${type}-api-keys-container .api-key-input`).forEach(i => i.disabled = false);
            document.getElementById(`${type}-add-api-key`) && (document.getElementById(`${type}-add-api-key`).disabled = false);
            checkStartBtn(type);
        } else if (type === 'news') {
            ['agent1', 'agent2', 'agent3'].forEach(a => {
                const inp = document.getElementById(`news-api-key-${a}`);
                if (inp) inp.disabled = false;
            });
            checkNewsKeys();
        } else {
            const inp = document.getElementById(`${type}-api-key`);
            if (inp) inp.disabled = false;
        }
    }

    function updateUIForProcessing(type) {
        showProgressSection(type, true);
        const s = document.getElementById(`${type}-start-btn`); if (s) s.disabled = true;
        const p = document.getElementById(`${type}-pause-btn`); if (p) p.disabled = false;
        const r = document.getElementById(`${type}-resume-btn`); if (r) r.disabled = true;
        const f = document.getElementById(`${type}-finish-btn`); if (f) f.disabled = true;
        const e = document.getElementById(`${type}-end-btn`); if (e) e.disabled = false;
        const a = document.getElementById(`${type}-download-btn`); if (a) a.disabled = true;
        disableAllKeys(type);
    }
    function updateUIForPaused(type) {
        const p = document.getElementById(`${type}-pause-btn`); if (p) p.disabled = true;
        const r = document.getElementById(`${type}-resume-btn`); if (r) r.disabled = false;
        const f = document.getElementById(`${type}-finish-btn`); if (f) f.disabled = false;
        const e = document.getElementById(`${type}-end-btn`); if (e) e.disabled = false;
    }
    function updateUIForDone(type) {
        showProgressSection(type, true);
        const s = document.getElementById(`${type}-start-btn`); if (s) s.disabled = true;
        const p = document.getElementById(`${type}-pause-btn`); if (p) p.disabled = true;
        const r = document.getElementById(`${type}-resume-btn`); if (r) r.disabled = true;
        const f = document.getElementById(`${type}-finish-btn`); if (f) f.disabled = false;
        const e = document.getElementById(`${type}-end-btn`); if (e) e.disabled = false;
        const a = document.getElementById(`${type}-download-btn`); if (a) a.disabled = false;
        updateSteps(type, 'result');
        loadStatistics(type);
        showToast('任务完成！正在加载结果...', 'success');
    }
    function updateUIForError(type) {
        const s = document.getElementById(`${type}-start-btn`); if (s) s.disabled = false;
        const p = document.getElementById(`${type}-pause-btn`); if (p) p.disabled = true;
        const r = document.getElementById(`${type}-resume-btn`); if (r) r.disabled = true;
        const f = document.getElementById(`${type}-finish-btn`); if (f) f.disabled = true;
        const e = document.getElementById(`${type}-end-btn`); if (e) e.disabled = false;
        const a = document.getElementById(`${type}-download-btn`); if (a) a.disabled = true;
        enableAllKeys(type);
    }
    function updateUIForReset(type) {
        showProgressSection(type, false);
        const bar = document.getElementById(`${type}-progress-bar-fill`);
        if (bar) bar.style.width = '0%';
        const pct = document.getElementById(`${type}-progress-pct`);
        if (pct) pct.textContent = '0%';
        const txt = document.getElementById(`${type}-progress-text`);
        if (txt) txt.textContent = '0 / 0 条';
        const speed = document.getElementById(`${type}-speed`);
        if (speed) speed.textContent = '-';
        const eta = document.getElementById(`${type}-eta`);
        if (eta) eta.textContent = '-';
        const rc = document.getElementById(`${type}-result-container`);
        if (rc) rc.classList.add('hidden');
        ['start', 'pause', 'resume', 'finish', 'end', 'download'].forEach(b => {
            const btn = document.getElementById(`${type}-${b}-btn`);
            if (btn) btn.disabled = true;
        });
        enableAllKeys(type);
        showStatusMessage(type, '任务已结束');
        updateSteps(type, 'api-key');
        startTimes[type] = null;
        const subtasksContainer = document.getElementById(`${type}-subtasks`);
        if (subtasksContainer) { subtasksContainer.innerHTML = ''; subtasksContainer.classList.add('hidden'); }
        showToast('任务已重置', 'info');
    }

    /* ========== Load Statistics ========== */
    function loadStatistics(type) {
        fetch(`/statistics/${type}?session_id=${sessionId}`)
            .then(r => r.json()).then(d => {
                if (d.error) { showToast(d.error, 'error'); return; }
                const rc = document.getElementById(`${type}-result-container`);
                if (rc) rc.classList.remove('hidden');
                setTimeout(() => {
                    renderResultChart(type, d.results);
                    renderTagChart(type, d.tags);
                }, 100);
            }).catch(() => showToast('获取统计数据失败', 'error'));
    }

    function renderResultChart(type, results) {
        const canvasId = `${type}-result-chart`;
        const canvas = document.getElementById(canvasId);
        if (!canvas) return;
        if (window[canvasId + '_chart']) window[canvasId + '_chart'].destroy();
        const colors = ['#00d4ff', '#00e59b', '#ffab40', '#ff5252', '#7b61ff', '#4caf50', '#e91e63'];
        window[canvasId + '_chart'] = new Chart(canvas, {
            type: 'doughnut',
            data: {
                labels: Object.keys(results),
                datasets: [{
                    data: Object.values(results),
                    backgroundColor: colors.slice(0, Object.keys(results).length),
                    borderWidth: 0
                }]
            },
            options: {
                responsive: true, maintainAspectRatio: false,
                plugins: { legend: { position: 'bottom', labels: { color: '#8a94a6', font: { size: 12 } } } }
            }
        });
    }

    function renderTagChart(type, tags) {
        const canvasId = `${type}-tag-chart`;
        const canvas = document.getElementById(canvasId);
        if (!canvas) return;
        if (window[canvasId + '_chart']) window[canvasId + '_chart'].destroy();
        window[canvasId + '_chart'] = new Chart(canvas, {
            type: 'bar',
            data: {
                labels: Object.keys(tags),
                datasets: [{
                    label: '数量',
                    data: Object.values(tags),
                    backgroundColor: 'rgba(0, 212, 255, 0.7)',
                    borderRadius: 4, barThickness: 28
                }]
            },
            options: {
                responsive: true, maintainAspectRatio: false,
                scales: {
                    y: { beginAtZero: true, ticks: { precision: 0, color: '#8a94a6' }, grid: { color: 'rgba(255,255,255,0.05)' } },
                    x: { ticks: { color: '#8a94a6' }, grid: { color: 'rgba(255,255,255,0.05)' } }
                },
                plugins: { legend: { display: false } }
            }
        });
    }

    /* ========== History ========== */
    let currentHistoryPage = 1;
    let totalHistoryPages = 1;
    const historyPerPage = 10;
    let currentStartDate = '';
    let currentEndDate = '';

    function initializeHistoryPage() {
        setDefaultTimeRange();
        initHistoryFilters();
        loadHistoryData(1);
        loadHistoryStats();
    }

    function setDefaultTimeRange() {
        const today = new Date();
        const ago = new Date(today);
        ago.setDate(today.getDate() - 7);
        const format = d => `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`;
        const sd = document.getElementById('start-date');
        const ed = document.getElementById('end-date');
        if (sd) { sd.value = format(ago); currentStartDate = sd.value; }
        if (ed) { ed.value = format(today); currentEndDate = ed.value; }
    }

    function initHistoryFilters() {
        document.querySelectorAll('.quick-filter-btn').forEach(btn => {
            btn.addEventListener('click', function() {
                document.querySelectorAll('.quick-filter-btn').forEach(b => b.classList.remove('active'));
                this.classList.add('active');
                const days = parseInt(this.dataset.days);
                const today = new Date();
                const format = d => `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`;
                const sd = document.getElementById('start-date');
                const ed = document.getElementById('end-date');
                if (days === 0) { sd.value = ''; ed.value = ''; currentStartDate = ''; currentEndDate = ''; }
                else {
                    const s = new Date(today); s.setDate(today.getDate() - days);
                    sd.value = format(s); ed.value = format(today);
                    currentStartDate = sd.value; currentEndDate = ed.value;
                }
                loadHistoryData(1);
                loadHistoryStats();
            });
        });
        document.getElementById('apply-filter').onclick = () => {
            const sd = document.getElementById('start-date');
            const ed = document.getElementById('end-date');
            currentStartDate = sd?.value || '';
            currentEndDate = ed?.value || '';
            loadHistoryData(1);
            loadHistoryStats();
        };
    }

    function loadHistoryData(page = 1) {
        let url = `/history/page?page=${page}&per_page=${historyPerPage}`;
        if (currentStartDate) url += `&start_date=${currentStartDate}`;
        if (currentEndDate) url += `&end_date=${currentEndDate}`;
        fetch(url).then(r => r.json()).then(d => {
            const list = document.getElementById('history-list');
            if (!list) return;
            if (!d.history || d.history.length === 0) {
                list.innerHTML = '<p style="text-align:center;color:var(--text-muted);padding:40px 0;">暂无历史记录</p>';
                const info = document.getElementById('history-page-info');
                if (info) info.textContent = '暂无数据';
                const prev = document.getElementById('history-prev-page');
                const next = document.getElementById('history-next-page');
                if (prev) prev.disabled = true;
                if (next) next.disabled = true;
                return;
            }
            currentHistoryPage = d.page;
            totalHistoryPages = d.pages;
            const info = document.getElementById('history-page-info');
            if (info) info.textContent = `第 ${currentHistoryPage} / ${totalHistoryPages} 页`;
            const prev = document.getElementById('history-prev-page');
            const next = document.getElementById('history-next-page');
            if (prev) prev.disabled = currentHistoryPage <= 1;
            if (next) next.disabled = currentHistoryPage >= totalHistoryPages;
            const typeNames = { comment: '评论审核', cover: '封面审核', push: '智慧Push审核', brand: '品牌守护审核', news: '资讯巡检' };
            list.innerHTML = d.history.map(r => {
                const typeText = typeNames[r.audit_type] || r.audit_type;
                const st = r.statistics?.results || {};
                const label = (r.audit_type === 'comment' || r.audit_type === 'push') ? '低质' : '违规';
                return `<div class="history-item">
                    <h4>${typeText} — ${r.filename || '未知文件'}</h4>
                    <p>时间：${r.datetime}</p>
                    <p>ID：${r.id}</p>
                    <p>总数：${r.total_rows || 0} | 正常：${st.正常 || 0} | ${label}：${st.低质 || st.违规 || 0} | 失败：${st.处理失败 || 0}</p>
                    <div class="history-actions">
                        <a href="/history/download/${r.id}" class="btn btn-ghost btn-sm" target="_blank">下载结果</a>
                        <button class="btn btn-ghost-danger btn-sm delete-history-btn" data-id="${r.id}">删除</button>
                    </div>
                </div>`;
            }).join('');
        }).catch(() => {});
    }

    function loadHistoryStats() {
        let url = '/history/statistics';
        const p = [];
        if (currentStartDate) p.push(`start_date=${currentStartDate}`);
        if (currentEndDate) p.push(`end_date=${currentEndDate}`);
        if (p.length) url += '?' + p.join('&');
        fetch(url).then(r => r.json()).then(d => {
            if (d.error) return;
            const byType = d.by_type || {};
            const set = (id, val) => { const el = document.getElementById(id); if (el) el.textContent = val; };
            set('stats-total', Object.values(byType).reduce((a, b) => a + b, 0));
            set('stats-comment', byType.comment || 0);
            set('stats-cover', byType.cover || 0);
            set('stats-push', byType.push || 0);
            set('stats-brand', byType.brand || 0);
            set('stats-news', byType.news || 0);
            const byVol = d.by_volume || {};
            set('volume-total', Object.values(byVol).reduce((a, b) => a + b, 0));
            set('volume-comment', byVol.comment || 0);
            set('volume-cover', byVol.cover || 0);
            set('volume-push', byVol.push || 0);
            set('volume-brand', byVol.brand || 0);
            set('volume-news', byVol.news || 0);
            renderHistoryTypeChart(byType);
            renderHistoryDateChart(d.by_date || {});
        }).catch(() => {});
    }

    function renderHistoryTypeChart(byType) {
        const canvas = document.getElementById('history-type-chart');
        if (!canvas) return;
        if (window.historyTypeChart) window.historyTypeChart.destroy();
        const labels = { comment: '评论', cover: '封面', push: 'Push', brand: '品牌', news: '资讯' };
        const colors = ['#00d4ff', '#00e59b', '#ffab40', '#ff5252', '#7b61ff'];
        const ks = Object.keys(byType);
        window.historyTypeChart = new Chart(canvas, {
            type: 'doughnut',
            data: {
                labels: ks.map(k => labels[k] || k),
                datasets: [{ data: ks.map(k => byType[k]), backgroundColor: colors, borderWidth: 0 }]
            },
            options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { position: 'bottom', labels: { color: '#8a94a6' } } } }
        });
    }

    function renderHistoryDateChart(byDate) {
        const canvas = document.getElementById('history-date-chart');
        if (!canvas) return;
        if (window.historyDateChart) window.historyDateChart.destroy();
        const labels = [], data = [];
        if (currentStartDate && currentEndDate) {
            const sd = new Date(currentStartDate), ed = new Date(currentEndDate);
            const diff = Math.ceil((ed - sd) / 86400000);
            for (let i = 0; i <= diff; i++) {
                const d = new Date(sd); d.setDate(sd.getDate() + i);
                const s = d.toISOString().split('T')[0];
                labels.push(s); data.push(byDate[s] || 0);
            }
        } else {
            Object.keys(byDate).sort().forEach(s => { labels.push(s); data.push(byDate[s] || 0); });
        }
        window.historyDateChart = new Chart(canvas, {
            type: 'line',
            data: {
                labels,
                datasets: [{ label: '任务数', data, borderColor: '#00d4ff', backgroundColor: 'rgba(0,212,255,0.08)', fill: true, tension: 0.3, pointRadius: 3, borderWidth: 2 }]
            },
            options: {
                responsive: true, maintainAspectRatio: false,
                scales: {
                    y: { beginAtZero: true, ticks: { precision: 0, color: '#8a94a6' }, grid: { color: 'rgba(255,255,255,0.05)' } },
                    x: { ticks: { color: '#8a94a6' }, grid: { color: 'rgba(255,255,255,0.05)' } }
                },
                plugins: { legend: { display: false } }
            }
        });
    }

    document.getElementById('history-prev-page').onclick = () => { if (currentHistoryPage > 1) loadHistoryData(currentHistoryPage - 1); };
    document.getElementById('history-next-page').onclick = () => { if (currentHistoryPage < totalHistoryPages) loadHistoryData(currentHistoryPage + 1); };

    document.getElementById('history-export-json').onclick = () => {
        let url = '/history/export?format=json';
        if (currentStartDate) url += `&start_date=${currentStartDate}`;
        if (currentEndDate) url += `&end_date=${currentEndDate}`;
        window.location.href = url;
        showToast('开始导出...', 'info');
    };

    document.getElementById('history-restore-btn').onclick = async () => {
        const ok = await showConfirm('恢复历史数据', '将扫描服务器上的文件并恢复缺失的历史记录，是否继续？');
        if (!ok) return;
        showToast('正在扫描文件...', 'info');
        const btn = document.getElementById('history-restore-btn');
        btn.disabled = true;
        fetch('/history/scan-and-restore', { method: 'POST' })
            .then(r => {
                if (!r.ok) throw new Error('恢复失败');
                return r.json();
            })
            .then(d => {
                if (d.restored === 0) {
                    showToast('没有发现可恢复的记录', 'info');
                } else {
                    showToast(`成功恢复 ${d.restored} 条历史记录`, 'success');
                    loadHistoryData(1);
                    loadHistoryStats();
                }
            })
            .catch(e => showToast(e.message, 'error'))
            .finally(() => { btn.disabled = false; });
    };

    /* Delete history */
    document.getElementById('history-list').addEventListener('click', async e => {
        const btn = e.target.closest('.delete-history-btn');
        if (!btn) return;
        const hid = btn.dataset.id;
        const ok = await showConfirm('删除确认', `确定要删除ID为 ${hid} 的历史记录吗？此操作不可撤销。`);
        if (!ok) return;
        fetch(`/history/${hid}`, { method: 'DELETE' })
            .then(r => { if (!r.ok) throw new Error('删除失败'); return r.json(); })
            .then(() => { showToast('删除成功', 'success'); loadHistoryData(currentHistoryPage); loadHistoryStats(); })
            .catch(e => showToast(e.message, 'error'));
    });

    /* ========== Settings ========== */
    function loadSettings() {
        fetch('/api/settings')
            .then(r => { if (r.ok) return r.json(); throw new Error('No settings API'); })
            .then(data => {
                if (!data) return;
                const u = document.getElementById('setting-platform-url');
                if (u && data.platform_url) u.value = data.platform_url;
                const keys = data.api_keys || {};
                const set = (id, val) => { const el = document.getElementById(id); if (el) el.value = Array.isArray(val) ? val.join(', ') : (val || ''); };
                set('setting-key-comment', keys.comment);
                set('setting-key-push', keys.push);
                set('setting-key-cover', keys.cover);
                set('setting-key-brand', keys.brand);
                set('setting-key-news1', keys.news_info);
                set('setting-key-news2', keys.news_image);
                set('setting-key-news3', keys.news_text);
                showToast('配置已加载', 'info');
            })
            .catch(() => {
                const u = document.getElementById('setting-platform-url');
                if (u) u.value = 'http://10.101.2.49:5000';
            });
    }

    document.getElementById('settings-save-btn')?.addEventListener('click', () => {
        const platformUrl = document.getElementById('setting-platform-url')?.value || '';
        const keys = {
            comment: (document.getElementById('setting-key-comment')?.value || '').split(',').map(s => s.trim()).filter(Boolean),
            push: (document.getElementById('setting-key-push')?.value || '').split(',').map(s => s.trim()).filter(Boolean),
            cover: (document.getElementById('setting-key-cover')?.value || '').split(',').map(s => s.trim()).filter(Boolean),
            brand: document.getElementById('setting-key-brand')?.value?.trim() || '',
            news_info: document.getElementById('setting-key-news1')?.value?.trim() || '',
            news_image: document.getElementById('setting-key-news2')?.value?.trim() || '',
            news_text: document.getElementById('setting-key-news3')?.value?.trim() || ''
        };
        fetch('/api/settings', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ platform_url: platformUrl, api_keys: keys })
        })
            .then(r => {
                if (!r.ok) throw new Error('保存失败');
                return r.json();
            })
            .then(() => showToast('配置已保存', 'success'))
            .catch(e => showToast(e.message, 'error'));
    });

    document.getElementById('settings-export-btn')?.addEventListener('click', () => {
        const data = {
            platform_url: document.getElementById('setting-platform-url')?.value || '',
            api_keys: {
                comment: (document.getElementById('setting-key-comment')?.value || '').split(',').map(s => s.trim()).filter(Boolean),
                push: (document.getElementById('setting-key-push')?.value || '').split(',').map(s => s.trim()).filter(Boolean),
                cover: (document.getElementById('setting-key-cover')?.value || '').split(',').map(s => s.trim()).filter(Boolean),
                brand: document.getElementById('setting-key-brand')?.value?.trim() || '',
                news_info: document.getElementById('setting-key-news1')?.value?.trim() || '',
                news_image: document.getElementById('setting-key-news2')?.value?.trim() || '',
                news_text: document.getElementById('setting-key-news3')?.value?.trim() || ''
            }
        };
        const blob = new Blob([JSON.stringify(data, null, 2)], { type: 'application/json' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `config_${new Date().toISOString().split('T')[0]}.json`;
        a.click();
        URL.revokeObjectURL(url);
        showToast('配置已导出', 'success');
    });

    document.getElementById('settings-import-btn')?.addEventListener('click', () => {
        document.getElementById('settings-import-file').click();
    });
    document.getElementById('settings-import-file')?.addEventListener('change', function() {
        if (this.files.length === 0) return;
        const reader = new FileReader();
        reader.onload = e => {
            try {
                const data = JSON.parse(e.target.result);
                const u = document.getElementById('setting-platform-url');
                if (u && data.platform_url) u.value = data.platform_url;
                const keys = data.api_keys || {};
                const set = (id, val) => { const el = document.getElementById(id); if (el) el.value = Array.isArray(val) ? val.join(', ') : (val || ''); };
                set('setting-key-comment', keys.comment);
                set('setting-key-push', keys.push);
                set('setting-key-cover', keys.cover);
                set('setting-key-brand', keys.brand);
                set('setting-key-news1', keys.news_info);
                set('setting-key-news2', keys.news_image);
                set('setting-key-news3', keys.news_text);
                showToast('导入成功，请点击保存', 'success');
            } catch (err) {
                showToast('文件格式错误', 'error');
            }
        };
        reader.readAsText(this.files[0]);
        this.value = '';
    });

    /* Copy helper for settings */
    window.copySettingKey = function(type) {
        const el = document.getElementById(`setting-key-${type}`);
        if (el && el.value) {
            navigator.clipboard?.writeText(el.value).then(() => showToast('已复制到剪贴板', 'success'));
        }
    };

    /* ========== Init ========== */
    const activeTab = document.querySelector('.nav-item.active');
    if (activeTab) switchTab(activeTab.dataset.tab);
    else switchTab('comment');
});
