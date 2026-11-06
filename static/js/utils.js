const Utils = {
    riskColor(score) {
        if (score <= 15) return Theme.c('green');
        if (score <= 35) return Theme.c('amber');
        if (score <= 60) return Theme.c('orange');
        return Theme.c('red');
    },

    riskBg(score) {
        if (score <= 15) return Theme.c('green', 0.15);
        if (score <= 35) return Theme.c('amber', 0.15);
        if (score <= 60) return Theme.c('orange', 0.15);
        return Theme.c('red', 0.15);
    },

    riskLabel(score) {
        if (score <= 15) return 'Low';
        if (score <= 35) return 'Moderate';
        if (score <= 60) return 'High';
        return 'Critical';
    },

    INDICATOR_LABELS: {
        'political_stability': 'Political Stability',
        'military_conflict': 'Military Conflict',
        'economic_sanctions': 'Economic Sanctions',
        'protests_civil_unrest': 'Protests / Unrest',
        'terrorism': 'Terrorism',
        'diplomatic_tensions': 'Diplomatic Tensions'
    },

    INDICATOR_ORDER: [
        'military_conflict',
        'political_stability',
        'terrorism',
        'protests_civil_unrest',
        'economic_sanctions',
        'diplomatic_tensions'
    ],

    timeAgo(dateStr) {
        if (!dateStr) return '';
        const now = new Date();
        const date = new Date(dateStr);
        const diff = Math.floor((now - date) / 1000);

        if (diff < 60) return 'just now';
        if (diff < 3600) return Math.floor(diff / 60) + 'm ago';
        if (diff < 86400) return Math.floor(diff / 3600) + 'h ago';
        return Math.floor(diff / 86400) + 'd ago';
    },

    escapeHtml(text) {
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }
};
