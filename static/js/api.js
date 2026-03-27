const ApiClient = {
    BASE_URL: '/api',

    async getScores() {
        try {
            const res = await fetch(`${this.BASE_URL}/scores`);
            if (!res.ok) return {};
            return await res.json();
        } catch (e) {
            console.warn('Failed to fetch scores:', e);
            return {};
        }
    },

    async getCountryDetail(countryCode) {
        try {
            const res = await fetch(`${this.BASE_URL}/scores/${countryCode}`);
            if (!res.ok) return null;
            return await res.json();
        } catch (e) {
            console.warn(`Failed to fetch detail for ${countryCode}:`, e);
            return null;
        }
    },

    async getHeadlines(countryCode) {
        try {
            const res = await fetch(`${this.BASE_URL}/headlines/${countryCode}`);
            if (!res.ok) return { articles: [] };
            return await res.json();
        } catch (e) {
            console.warn(`Failed to fetch headlines for ${countryCode}:`, e);
            return { articles: [] };
        }
    },

    async getGlobalHeadlines() {
        try {
            const res = await fetch(`${this.BASE_URL}/headlines/global`);
            if (!res.ok) return { articles: [] };
            return await res.json();
        } catch (e) {
            console.warn('Failed to fetch global headlines:', e);
            return { articles: [] };
        }
    },

    async getHotspots() {
        try {
            const res = await fetch(`${this.BASE_URL}/hotspots`);
            if (!res.ok) return { hotspots: [] };
            return await res.json();
        } catch (e) {
            console.warn('Failed to fetch hotspots:', e);
            return { hotspots: [] };
        }
    },

    async getStatus() {
        try {
            const res = await fetch(`${this.BASE_URL}/status`);
            if (!res.ok) return {};
            return await res.json();
        } catch (e) {
            console.warn('Failed to fetch status:', e);
            return {};
        }
    },

    async getCountryHistory(countryCode, days = 90) {
        try {
            const res = await fetch(`${this.BASE_URL}/history/${countryCode}?days=${days}`);
            if (!res.ok) return { series: [] };
            return await res.json();
        } catch (e) {
            console.warn(`Failed to fetch history for ${countryCode}:`, e);
            return { series: [] };
        }
    }
};
