/**
 * Parra Economist — Chat frontend
 */
(function () {
    const messagesEl = document.getElementById('chatMessages');
    const inputEl = document.getElementById('chatInput');
    const sendBtn = document.getElementById('sendBtn');
    const welcomeEl = document.getElementById('welcomeState');

    let conversationHistory = [];
    let isStreaming = false;

    // --- Init ---
    function init() {
        sendBtn.addEventListener('click', sendMessage);
        inputEl.addEventListener('keydown', (e) => {
            if (e.key === 'Enter' && !e.shiftKey) {
                e.preventDefault();
                sendMessage();
            }
        });
        inputEl.addEventListener('input', () => {
            autoResize();
            sendBtn.disabled = !inputEl.value.trim() || isStreaming;
        });

        // Suggested question pills
        document.querySelectorAll('.suggestion-pill').forEach(btn => {
            btn.addEventListener('click', () => {
                inputEl.value = btn.dataset.question;
                sendMessage();
            });
        });
    }

    function autoResize() {
        inputEl.style.height = 'auto';
        inputEl.style.height = Math.min(inputEl.scrollHeight, 120) + 'px';
    }

    // --- Send Message ---
    function sendMessage() {
        const text = inputEl.value.trim();
        if (!text || isStreaming) return;

        // Hide welcome state
        if (welcomeEl) {
            welcomeEl.style.display = 'none';
        }

        // Add user message
        appendMessage('user', text);
        conversationHistory.push({ role: 'user', content: text });

        // Clear input
        inputEl.value = '';
        inputEl.style.height = 'auto';
        sendBtn.disabled = true;
        isStreaming = true;

        // Create assistant placeholder
        const { textEl } = appendMessage('assistant', '', true);

        // Stream response
        streamResponse(textEl);
    }

    // --- Append Message to DOM ---
    function appendMessage(role, content, isLoading = false) {
        const msg = document.createElement('div');
        msg.className = `message ${role}`;

        const avatar = document.createElement('div');
        avatar.className = 'message-avatar';
        avatar.textContent = role === 'user' ? 'You' : 'PE';

        const body = document.createElement('div');
        body.className = 'message-content';

        const label = document.createElement('div');
        label.className = 'message-role';
        label.textContent = role === 'user' ? 'You' : 'Parra Economist';

        const textEl = document.createElement('div');
        textEl.className = 'message-text';

        if (isLoading) {
            textEl.innerHTML = '<div class="loading-indicator"><span class="dot"></span><span class="dot"></span><span class="dot"></span></div>';
        } else {
            textEl.innerHTML = renderMarkdown(content);
        }

        body.appendChild(label);
        body.appendChild(textEl);
        msg.appendChild(avatar);
        msg.appendChild(body);
        messagesEl.appendChild(msg);
        scrollToBottom();

        return { msg, textEl };
    }

    // --- Stream Response from SSE ---
    async function streamResponse(textEl) {
        let fullText = '';

        try {
            const res = await fetch('/api/economist/chat', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ messages: conversationHistory })
            });

            if (!res.ok) {
                const err = await res.json().catch(() => ({ error: 'Request failed' }));
                textEl.innerHTML = `<span class="message-error">${err.error || 'Something went wrong. Please try again.'}</span>`;
                isStreaming = false;
                sendBtn.disabled = !inputEl.value.trim();
                return;
            }

            const reader = res.body.getReader();
            const decoder = new TextDecoder();
            let buffer = '';

            while (true) {
                const { done, value } = await reader.read();
                if (done) break;

                buffer += decoder.decode(value, { stream: true });

                // Parse SSE events from buffer
                const lines = buffer.split('\n');
                buffer = lines.pop(); // keep incomplete line in buffer

                for (const line of lines) {
                    if (!line.startsWith('data: ')) continue;
                    const jsonStr = line.slice(6);
                    try {
                        const event = JSON.parse(jsonStr);
                        if (event.type === 'text') {
                            fullText += event.content;
                            textEl.innerHTML = renderMarkdown(fullText);
                            scrollToBottom();
                        } else if (event.type === 'error') {
                            textEl.innerHTML = `<span class="message-error">${event.content}</span>`;
                        } else if (event.type === 'done') {
                            // done
                        }
                    } catch (e) {
                        // skip malformed JSON
                    }
                }
            }
        } catch (err) {
            textEl.innerHTML = `<span class="message-error">Connection error. Please try again.</span>`;
            fullText = '';
        }

        if (fullText) {
            conversationHistory.push({ role: 'assistant', content: fullText });
        }

        isStreaming = false;
        sendBtn.disabled = !inputEl.value.trim();
    }

    // --- Lightweight Markdown Rendering ---
    function renderMarkdown(text) {
        if (!text) return '';

        let html = escapeHtml(text);

        // Bold: **text**
        html = html.replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>');

        // Italic: *text*
        html = html.replace(/\*(.+?)\*/g, '<em>$1</em>');

        // Inline code: `code`
        html = html.replace(/`(.+?)`/g, '<code>$1</code>');

        // Unordered lists: lines starting with - or *
        html = html.replace(/^[\-\*] (.+)$/gm, '<li>$1</li>');
        html = html.replace(/((?:<li>.*<\/li>\n?)+)/g, '<ul>$1</ul>');

        // Ordered lists: lines starting with 1. 2. etc.
        html = html.replace(/^\d+\.\s(.+)$/gm, '<li>$1</li>');
        // Wrap consecutive <li> that aren't already in <ul>
        html = html.replace(/(?<!<\/ul>)((?:<li>.*<\/li>\n?)+)(?!<\/ul>)/g, (match) => {
            if (match.includes('<ul>')) return match;
            return '<ol>' + match + '</ol>';
        });

        // Headers: ### text
        html = html.replace(/^### (.+)$/gm, '<strong>$1</strong>');
        html = html.replace(/^## (.+)$/gm, '<strong>$1</strong>');

        // Paragraphs: double newlines
        html = html.replace(/\n\n/g, '</p><p>');
        html = '<p>' + html + '</p>';

        // Clean up empty paragraphs
        html = html.replace(/<p>\s*<\/p>/g, '');

        // Single newlines within paragraphs → <br>
        html = html.replace(/\n/g, '<br>');

        return html;
    }

    function escapeHtml(str) {
        const div = document.createElement('div');
        div.textContent = str;
        return div.innerHTML;
    }

    function scrollToBottom() {
        messagesEl.scrollTop = messagesEl.scrollHeight;
    }

    // Start
    init();
})();
