function app() {
  const BACKEND = window.__BACKEND_URL || '';

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

    _pollTimer: null,

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
        const r = await fetch(BACKEND + '/api/config');
        this.config = await r.json();
      } catch (e) {
        this.toast('Failed to load config', 'error');
      }
    },

    async loadSheets() {
      try {
        const r = await fetch(BACKEND + '/api/sheets');
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
        const r = await fetch(BACKEND + '/api/status/' + encodeURIComponent(tab));
        this.email.stats = await r.json();
      } catch { this.email.stats = null; }
    },

    async loadDashboard() {
      this.dashboard.loading = true;
      await this.loadSheets();
      try {
        const r = await fetch(BACKEND + '/api/stats/all');
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

    async startPipeline() {
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

      try {
        const r = await fetch(BACKEND + '/api/pipeline/start', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            country: this.pipeline.country || null,
            city: this.pipeline.city || null,
            niche: this.pipeline.niche || null,
            send_emails: this.pipeline.sendEmails,
          }),
        });
        const d = await r.json();
        if (!d.started) {
          this.toast(d.error || 'Failed to start pipeline', 'error');
          this.pipeline.running = false;
          return;
        }
        this._startPolling('pipeline');
      } catch (e) {
        this.toast('Failed to start pipeline: ' + e.message, 'error');
        this.pipeline.running = false;
      }
    },

    /* ── Email ────────────────────────────── */

    async startEmail() {
      if (this.email.running) return;
      this.emailLogs = [];
      this.email.result = null;
      this.email.running = true;

      try {
        const r = await fetch(BACKEND + '/api/email/start', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            sheet_tab: this.email.sheetTab || null,
          }),
        });
        const d = await r.json();
        if (!d.started) {
          this.toast(d.error || 'Failed to start email', 'error');
          this.email.running = false;
          return;
        }
        this._startPolling('email');
      } catch (e) {
        this.toast('Failed to start email: ' + e.message, 'error');
        this.email.running = false;
      }
    },

    /* ── Polling ──────────────────────────── */

    _startPolling(job) {
      let logIndex = 0;
      const poll = async () => {
        try {
          const r = await fetch(BACKEND + `/api/logs/${job}?since=${logIndex}`);
          const d = await r.json();

          if (d.logs && d.logs.length > 0) {
            const targetLogs = job === 'pipeline' ? 'pipelineLogs' : 'emailLogs';
            for (const log of d.logs) {
              this[targetLogs].push(log);
              if (job === 'pipeline') {
                const detected = this.detectStep(log.message);
                if (detected !== null) this.pipeline.currentStep = detected;
              }
            }
            logIndex = d.total;
            this.$nextTick(() => this.scrollLog(job === 'pipeline' ? 'pipeline-log' : 'email-log'));
          }

          if (!d.running && d.result) {
            if (job === 'pipeline') {
              if (d.result.status === 'completed') {
                this.pipeline.currentStep = this.pipeline.sendEmails ? 5 : 4;
                this.pipeline.result = d.result.data;
                this.toast('Pipeline completed successfully', 'success');
              } else if (d.result.status === 'error') {
                this.toast('Pipeline error: ' + d.result.message, 'error');
              } else if (d.result.status === 'cancelled') {
                this.toast('Pipeline cancelled', 'info');
              }
              this.pipeline.running = false;
            } else {
              this.email.running = false;
              if (d.result.status === 'completed') {
                this.email.result = d.result.data;
                this.toast(`Sent ${d.result.data?.emails_sent ?? 0} emails`, 'success');
                this.loadTabStats();
              } else if (d.result.status === 'error') {
                this.toast('Email error: ' + d.result.message, 'error');
              } else if (d.result.status === 'cancelled') {
                this.toast('Email sending cancelled', 'info');
              }
            }
            return;
          }

          setTimeout(poll, 1500);
        } catch (e) {
          setTimeout(poll, 3000);
        }
      };
      setTimeout(poll, 500);
    },

    /* ── Stop ─────────────────────────────── */

    async stopJob(job) {
      try {
        await fetch(BACKEND + '/api/stop/' + job, { method: 'POST' });
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
