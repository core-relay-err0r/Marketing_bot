function app() {
  return {
    tab: 'pipeline',
    tabs: [
      { id: 'pipeline', label: 'Run Pipeline' },
      { id: 'email',    label: 'Send Emails' },
      { id: 'dashboard', label: 'Dashboard' },
    ],

    config: { countries: {}, niches: [], niche_priority: [] },

    pipeline: { country: '', city: '', niche: '', sendEmails: false, running: false, result: null, currentStep: -1 },

    pipelineSteps: [
      { id: 0, label: 'Scraping',       desc: 'Finding businesses on Google Maps' },
      { id: 1, label: 'Qualifying',     desc: 'Checking websites for weaknesses' },
      { id: 2, label: 'Deduplicating',  desc: 'Removing duplicates from tracker' },
      { id: 3, label: 'Adding to Sheet', desc: 'Writing leads to Google Sheets' },
      { id: 4, label: 'Sending Emails', desc: 'Sending outreach emails' },
    ],
    email:    { sheetTab: '', tabs: [], running: false, result: null, stats: null },
    dashboard: { totalLeads: null, totalEmailed: null, totalPending: null, sheets: [], sheetCount: null, dayStats: [], loading: false },

    pipelineLogs: [],
    emailLogs: [],
    toasts: [],

    _pipelineWs: null,
    _emailWs: null,

    async init() {
      await this.loadConfig();
      await this.loadSheets();
    },

    /* ── Validation helpers ───────────────── */

    isKnownCountry() {
      return Object.keys(this.config.countries || {}).some(
        c => c.toLowerCase() === (this.pipeline.country || '').toLowerCase()
      );
    },

    _matchedCountryKey() {
      return Object.keys(this.config.countries || {}).find(
        c => c.toLowerCase() === (this.pipeline.country || '').toLowerCase()
      ) || '';
    },

    getCitySuggestions() {
      const key = this._matchedCountryKey();
      if (!key) return [];
      return this.config.countries[key]?.cities || [];
    },

    isKnownCity() {
      const cities = this.getCitySuggestions();
      return cities.some(c => c.toLowerCase() === (this.pipeline.city || '').toLowerCase());
    },

    isKnownNiche() {
      return (this.config.niches || []).some(
        n => n.toLowerCase() === (this.pipeline.niche || '').toLowerCase()
      );
    },

    onCountryChange() {
      this.pipeline.city = '';
    },

    onCityChange() {
      if (this.pipeline.city && !this.pipeline.country) {
        const allCountries = this.config.countries || {};
        for (const [country, data] of Object.entries(allCountries)) {
          if ((data.cities || []).some(c => c.toLowerCase() === this.pipeline.city.toLowerCase())) {
            this.pipeline.country = country;
            break;
          }
        }
      }
    },

    canRunPipeline() {
      if (this.pipeline.running) return false;
      if (this.pipeline.city && !this.pipeline.country) return false;
      return true;
    },

    /* ── Data loading ─────────────────────── */

    async loadConfig() {
      try {
        const r = await fetch('/api/config');
        this.config = await r.json();
      } catch (e) {
        this.toast('Failed to load config', 'error');
      }
    },

    async loadSheets() {
      try {
        const r = await fetch('/api/sheets');
        const d = await r.json();
        this.email.tabs = d.sheets || [];
        this.dashboard.sheets = d.sheets || [];
        this.dashboard.sheetCount = (d.sheets || []).length;
        if (d.error) this.toast('Sheets: ' + d.error, 'error');
      } catch (e) {
        this.toast('Failed to load sheets', 'error');
      }
    },

    async loadTabStats() {
      const tab = this.email.sheetTab;
      if (!tab) { this.email.stats = null; return; }
      try {
        const r = await fetch('/api/status/' + encodeURIComponent(tab));
        this.email.stats = await r.json();
      } catch { this.email.stats = null; }
    },

    async loadDashboard() {
      this.dashboard.loading = true;
      await this.loadSheets();
      try {
        const r = await fetch('/api/stats/all');
        const d = await r.json();
        if (d.stats) {
          this.dashboard.dayStats = d.stats;
          this.dashboard.totalLeads = d.stats.reduce((s, r) => s + r.total, 0);
          this.dashboard.totalEmailed = d.stats.reduce((s, r) => s + r.emailed, 0);
          this.dashboard.totalPending = d.stats.reduce((s, r) => s + r.pending, 0);
        }
        if (d.error) this.toast('Stats: ' + d.error, 'error');
      } catch (e) {
        this.toast('Failed to load dashboard stats', 'error');
      }
      this.dashboard.loading = false;
    },

    /* ── Pipeline ─────────────────────────── */

    detectStep(msg) {
      const m = (msg || '').toLowerCase();
      if (m.includes('scraping:') || m.includes('pipeline targets'))          return 0;
      if (m.includes('qualifying businesses'))                                return 1;
      if (m.includes('deduplicating against'))                                return 2;
      if (m.includes('writing leads to google') || m.includes('appended'))    return 3;
      if (m.includes('sending outreach emails'))                              return 4;
      return null;
    },

    stepState(stepId) {
      if (this.pipeline.currentStep < 0)  return 'pending';
      if (stepId < this.pipeline.currentStep) return 'completed';
      if (stepId === this.pipeline.currentStep) return 'active';
      return 'pending';
    },

    startPipeline() {
      if (!this.canRunPipeline()) {
        if (this.pipeline.city && !this.pipeline.country) {
          this.toast('Please enter a country for the city', 'error');
        }
        return;
      }
      this.pipelineLogs = [];
      this.pipeline.result = null;
      this.pipeline.running = true;
      this.pipeline.currentStep = 0;

      const proto = location.protocol === 'https:' ? 'wss' : 'ws';
      this._pipelineWs = new WebSocket(`${proto}://${location.host}/ws/pipeline`);

      this._pipelineWs.onopen = () => {
        this._pipelineWs.send(JSON.stringify({
          country: this.pipeline.country || null,
          city: this.pipeline.city || null,
          niche: this.pipeline.niche || null,
          send_emails: this.pipeline.sendEmails,
        }));
      };

      this._pipelineWs.onmessage = (e) => {
        const data = JSON.parse(e.data);
        if (data.type === 'log') {
          this.pipelineLogs.push(data);
          const detected = this.detectStep(data.message);
          if (detected !== null) this.pipeline.currentStep = detected;
          this.$nextTick(() => this.scrollLog('pipeline-log'));
        } else if (data.type === 'result') {
          if (data.status === 'completed') {
            this.pipeline.currentStep = this.pipeline.sendEmails ? 5 : 4;
            this.pipeline.result = data.data;
            this.toast('Pipeline completed successfully', 'success');
          } else if (data.status === 'error') {
            this.toast('Pipeline error: ' + data.message, 'error');
          } else if (data.status === 'cancelled') {
            this.toast('Pipeline cancelled', 'info');
          }
          this.pipeline.running = false;
        } else if (data.type === 'status') {
          this.pipelineLogs.push({ ts: this.now(), level: 'INFO', message: data.message });
        }
      };

      this._pipelineWs.onerror = () => {
        this.pipeline.running = false;
        this.toast('WebSocket connection error', 'error');
      };

      this._pipelineWs.onclose = () => {
        this.pipeline.running = false;
      };
    },

    /* ── Email ────────────────────────────── */

    startEmail() {
      if (this.email.running) return;
      this.emailLogs = [];
      this.email.result = null;
      this.email.running = true;

      const proto = location.protocol === 'https:' ? 'wss' : 'ws';
      this._emailWs = new WebSocket(`${proto}://${location.host}/ws/email`);

      this._emailWs.onopen = () => {
        this._emailWs.send(JSON.stringify({
          sheet_tab: this.email.sheetTab || null,
        }));
      };

      this._emailWs.onmessage = (e) => {
        const data = JSON.parse(e.data);
        if (data.type === 'log') {
          this.emailLogs.push(data);
          this.$nextTick(() => this.scrollLog('email-log'));
        } else if (data.type === 'result') {
          this.email.running = false;
          if (data.status === 'completed') {
            this.email.result = data.data;
            this.toast(`Sent ${data.data?.emails_sent ?? 0} emails`, 'success');
            this.loadTabStats();
          } else if (data.status === 'error') {
            this.toast('Email error: ' + data.message, 'error');
          } else if (data.status === 'cancelled') {
            this.toast('Email sending cancelled', 'info');
          }
        } else if (data.type === 'status') {
          this.emailLogs.push({ ts: this.now(), level: 'INFO', message: data.message });
        }
      };

      this._emailWs.onerror = () => {
        this.email.running = false;
        this.toast('WebSocket connection error', 'error');
      };

      this._emailWs.onclose = () => {
        this.email.running = false;
      };
    },

    /* ── Stop ─────────────────────────────── */

    async stopJob(job) {
      try {
        await fetch('/api/stop/' + job, { method: 'POST' });
        this.toast('Stopping ' + job + '…', 'info');
      } catch {}
    },

    /* ── Helpers ──────────────────────────── */

    logClass(level) {
      switch (level) {
        case 'ERROR':   return 'text-rose-400';
        case 'WARNING': return 'text-amber-400';
        case 'DEBUG':   return 'text-slate-500';
        default:        return 'text-slate-300';
      }
    },

    scrollLog(id) {
      const el = document.getElementById(id);
      if (el) el.scrollTop = el.scrollHeight;
    },

    now() {
      const d = new Date();
      return [d.getHours(), d.getMinutes(), d.getSeconds()]
        .map(n => String(n).padStart(2, '0')).join(':');
    },

    toast(message, type = 'info') {
      const t = { message, type, visible: true };
      this.toasts.push(t);
      setTimeout(() => { t.visible = false; }, 5000);
      setTimeout(() => {
        const idx = this.toasts.indexOf(t);
        if (idx > -1) this.toasts.splice(idx, 1);
      }, 5500);
    },
  };
}
