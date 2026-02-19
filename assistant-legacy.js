(function initLegacyAssistant() {
  "use strict";

  if (typeof window === "undefined" || typeof document === "undefined") {
    return;
  }

  const ROOT_CLASS = "legacy-assistant";
  const LAUNCHER_CLASS = "legacy-assistant__launcher";
  const PANEL_CLASS = "legacy-assistant__panel";
  const CLIENT_OPEN_EVENT = "cb-assistant-open-client";
  const STORAGE_KEY = "cb_assistant_mode";
  const API_PATH = "/api/assistant/chat";
  const MAX_TEXT_LENGTH = 2000;
  const FEMALE_VOICE_HINTS = [
    "female",
    "woman",
    "samantha",
    "victoria",
    "karen",
    "olga",
    "maria",
    "anna",
    "alena",
    "alice",
    "katya",
    "zira",
    "jenny",
  ];
  const MALE_VOICE_HINTS = ["male", "man", "alex", "david", "daniel", "george", "sergey", "pavel"];
  const NATURAL_VOICE_HINTS = ["natural", "neural", "premium", "enhanced", "wavenet", "online"];

  const userLang = /^ru\b/i.test((navigator && navigator.language) || "") ? "ru" : "en";
  const dictionary = {
    ru: {
      openLabel: "Открыть помощника",
      closeLabel: "Закрыть",
      title: "CBooster Помощник",
      subtitle: "Внутренние данные клиентов",
      textMode: "Текст",
      voiceMode: "Голос",
      chatHistoryLabel: "История сообщений",
      suggestionsLabel: "Подсказки",
      inputPlaceholder: "Например: Покажи топ-5 должников",
      thinking: "Думаю...",
      send: "Отправить",
      sending: "Отправка...",
      startMic: "Включить микрофон",
      stopMic: "Остановить микрофон",
      speakReply: "Озвучить ответ",
      stopSpeak: "Остановить",
      voiceUnsupported: "Ваш браузер не поддерживает голосовой ввод.",
      voiceNoSpeech: "Голос не распознан. Попробуйте еще раз.",
      voiceNotAllowed: "Доступ к микрофону запрещен браузером.",
      voiceFailed: "Не удалось обработать голосовой ввод.",
      micStartFailed: "Не удалось включить микрофон.",
      networkError: "Не удалось получить ответ ассистента.",
      greeting:
        "Привет. Я могу ответить по вашим клиентским данным: сводка, долги, просрочки, статус конкретного клиента.",
      defaultSuggestions: [
        "Сводка по клиентам",
        "Покажи топ-5 должников",
        "Сколько просроченных клиентов?",
        "Покажи клиента John Smith",
      ],
    },
    en: {
      openLabel: "Open assistant",
      closeLabel: "Close",
      title: "CBooster Assistant",
      subtitle: "Internal client data",
      textMode: "Text",
      voiceMode: "Voice",
      chatHistoryLabel: "Chat messages",
      suggestionsLabel: "Suggestions",
      inputPlaceholder: "For example: Show top 5 debtors",
      thinking: "Thinking...",
      send: "Send",
      sending: "Sending...",
      startMic: "Start Mic",
      stopMic: "Stop Mic",
      speakReply: "Speak Reply",
      stopSpeak: "Stop",
      voiceUnsupported: "Your browser does not support voice input.",
      voiceNoSpeech: "No speech detected. Please try again.",
      voiceNotAllowed: "Microphone permission is blocked by the browser.",
      voiceFailed: "Unable to process voice input.",
      micStartFailed: "Failed to start microphone.",
      networkError: "Failed to receive an assistant response.",
      greeting:
        "Hi. I can answer from your internal client data: summaries, debt, overdue status, and specific client details.",
      defaultSuggestions: [
        "Give me a client summary",
        "Show top 5 debtors",
        "How many overdue clients do we have?",
        "Show client John Smith",
      ],
    },
  };

  const t = dictionary[userLang];
  const avatarPath = "/assistant-avatar.svg";

  const speechRecognitionCtor = window.SpeechRecognition || window.webkitSpeechRecognition || null;
  let speechRecognition = null;
  let isOpen = false;
  let isSending = false;
  let isListening = false;
  let isSpeaking = false;
  let mode = readMode();
  let speechSequence = 0;

  const state = {
    messages: [
      {
        id: createId(),
        role: "assistant",
        text: t.greeting,
        mentions: [],
      },
    ],
    suggestions: t.defaultSuggestions.slice(),
    voiceError: "",
  };

  const root = document.createElement("div");
  root.className = ROOT_CLASS;
  root.setAttribute("aria-live", "polite");
  document.body.appendChild(root);

  render();

  function createId() {
    return `${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
  }

  function normalizeText(value) {
    return (value || "").toString().replace(/\s+/g, " ").trim();
  }

  function normalizeMentionList(rawMentions) {
    if (!Array.isArray(rawMentions)) {
      return [];
    }

    const seen = new Set();
    const mentions = [];

    for (const item of rawMentions) {
      const mention = normalizeText(item);
      if (!mention) {
        continue;
      }

      const key = mention.toLowerCase();
      if (seen.has(key)) {
        continue;
      }

      seen.add(key);
      mentions.push(mention);
    }

    return mentions.sort(function sortByLength(left, right) {
      return right.length - left.length;
    });
  }

  function splitMessageByMentions(text, mentions) {
    const normalizedText = (text || "").toString();
    const mentionList = normalizeMentionList(mentions);
    if (!mentionList.length) {
      return [{ type: "text", text: normalizedText }];
    }

    const lowerText = normalizedText.toLowerCase();
    const parts = [];
    let cursor = 0;

    while (cursor < normalizedText.length) {
      let bestStart = -1;
      let bestMention = "";

      for (const mention of mentionList) {
        const start = lowerText.indexOf(mention.toLowerCase(), cursor);
        if (start === -1) {
          continue;
        }

        if (bestStart === -1 || start < bestStart || (start === bestStart && mention.length > bestMention.length)) {
          bestStart = start;
          bestMention = mention;
        }
      }

      if (bestStart === -1) {
        parts.push({ type: "text", text: normalizedText.slice(cursor) });
        break;
      }

      if (bestStart > cursor) {
        parts.push({ type: "text", text: normalizedText.slice(cursor, bestStart) });
      }

      const mentionText = normalizedText.slice(bestStart, bestStart + bestMention.length);
      parts.push({ type: "mention", text: mentionText, mention: mentionText });
      cursor = bestStart + bestMention.length;
    }

    return parts.filter(function onlyNonEmpty(part) {
      return Boolean(part && part.text);
    });
  }

  function dispatchOpenClientEvent(clientName) {
    const normalizedClientName = normalizeText(clientName);
    if (!normalizedClientName) {
      return;
    }

    window.dispatchEvent(
      new CustomEvent(CLIENT_OPEN_EVENT, {
        detail: {
          clientName: normalizedClientName,
        },
      }),
    );
  }

  function readMode() {
    try {
      const stored = window.localStorage.getItem(STORAGE_KEY);
      if (stored === "voice" || stored === "text") {
        return stored;
      }
    } catch {
      // ignore storage read errors
    }
    return "text";
  }

  function writeMode(nextMode) {
    try {
      window.localStorage.setItem(STORAGE_KEY, nextMode);
    } catch {
      // ignore storage write errors
    }
  }

  function scoreSpeechVoice(voice, isRussian) {
    const name = `${voice.name || ""} ${voice.voiceURI || ""}`.toLowerCase();
    const lang = (voice.lang || "").toLowerCase();
    let score = 0;

    if (isRussian) {
      if (lang.indexOf("ru") === 0) {
        score += 100;
      }
      if (lang.indexOf("ru-ru") === 0) {
        score += 16;
      }
    } else {
      if (lang.indexOf("en") === 0) {
        score += 100;
      }
      if (lang.indexOf("en-us") === 0) {
        score += 16;
      }
    }

    if (FEMALE_VOICE_HINTS.some(function hasFemaleHint(hint) { return name.includes(hint); })) {
      score += 42;
    }
    if (MALE_VOICE_HINTS.some(function hasMaleHint(hint) { return name.includes(hint); })) {
      score -= 30;
    }
    if (NATURAL_VOICE_HINTS.some(function hasNaturalHint(hint) { return name.includes(hint); })) {
      score += 18;
    }
    if (voice.localService === false) {
      score += 8;
    }
    if (voice.default) {
      score += 4;
    }

    return score;
  }

  function pickPreferredSpeechVoice(voices, isRussian) {
    if (!Array.isArray(voices) || !voices.length) {
      return null;
    }

    let bestVoice = null;
    let bestScore = -Infinity;
    for (const voice of voices) {
      const score = scoreSpeechVoice(voice, isRussian);
      if (score > bestScore) {
        bestScore = score;
        bestVoice = voice;
      }
    }

    return bestVoice;
  }

  function splitSpeechIntoChunks(rawText, maxLength) {
    const limit = Number.isFinite(maxLength) && maxLength > 0 ? Math.floor(maxLength) : 220;
    const normalized = (rawText || "").toString().replace(/\s+/g, " ").trim();
    if (!normalized) {
      return [];
    }

    const sentenceCandidates = normalized
      .replace(/([.!?;:])\s+/g, "$1\n")
      .split(/\n+/)
      .map(function trimChunk(item) { return item.trim(); })
      .filter(Boolean);

    const chunks = [];
    let current = "";

    function pushValue(value) {
      const text = (value || "").trim();
      if (!text) {
        return;
      }

      if (text.length <= limit) {
        chunks.push(text);
        return;
      }

      const commaParts = text
        .split(/,\s+/)
        .map(function trimPart(part) { return part.trim(); })
        .filter(Boolean);

      if (commaParts.length > 1) {
        for (const part of commaParts) {
          if (part.length <= limit) {
            chunks.push(part);
            continue;
          }

          const words = part.split(/\s+/);
          let buffer = "";
          for (const word of words) {
            const candidate = buffer ? `${buffer} ${word}` : word;
            if (candidate.length > limit) {
              if (buffer) {
                chunks.push(buffer);
              }
              buffer = word;
            } else {
              buffer = candidate;
            }
          }
          if (buffer) {
            chunks.push(buffer);
          }
        }
        return;
      }

      chunks.push(text);
    }

    for (const sentence of sentenceCandidates) {
      const candidate = current ? `${current} ${sentence}` : sentence;
      if (candidate.length > limit) {
        if (current) {
          pushValue(current);
        }
        current = sentence;
      } else {
        current = candidate;
      }
    }

    if (current) {
      pushValue(current);
    }

    return chunks;
  }

  function resolvePreferredSpeechVoice(isRussian) {
    if (!("speechSynthesis" in window)) {
      return Promise.resolve(null);
    }

    const synthesis = window.speechSynthesis;
    const immediate = pickPreferredSpeechVoice(synthesis.getVoices(), isRussian);
    if (immediate) {
      return Promise.resolve(immediate);
    }

    return new Promise(function waitForVoices(resolve) {
      let settled = false;
      function settle() {
        if (settled) {
          return;
        }
        settled = true;
        synthesis.removeEventListener("voiceschanged", handleVoicesChanged);
        window.clearTimeout(timeoutId);
        resolve(pickPreferredSpeechVoice(synthesis.getVoices(), isRussian));
      }
      function handleVoicesChanged() {
        settle();
      }
      const timeoutId = window.setTimeout(settle, 1200);
      synthesis.addEventListener("voiceschanged", handleVoicesChanged);
    });
  }

  function stopSpeaking(shouldRender) {
    speechSequence += 1;
    if ("speechSynthesis" in window) {
      window.speechSynthesis.cancel();
    }

    isSpeaking = false;
    if (shouldRender !== false) {
      renderActions();
    }
  }

  async function speakText(rawText) {
    const text = normalizeText(rawText);
    if (!text) {
      return;
    }

    if (!("speechSynthesis" in window)) {
      state.voiceError = userLang === "ru"
        ? "Ваш браузер не поддерживает голосовой синтез."
        : "Your browser does not support speech synthesis.";
      renderComposerOnly();
      return;
    }

    stopSpeaking(false);
    state.voiceError = "";
    renderComposerOnly();

    const sequence = speechSequence + 1;
    speechSequence = sequence;
    isSpeaking = true;
    renderActions();

    const chunks = splitSpeechIntoChunks(text, 220);
    if (!chunks.length) {
      isSpeaking = false;
      renderActions();
      return;
    }

    const synthesis = window.speechSynthesis;
    const preferredVoice = await resolvePreferredSpeechVoice(userLang === "ru");
    if (sequence !== speechSequence) {
      return;
    }

    const speechLang = (preferredVoice && preferredVoice.lang) || (userLang === "ru" ? "ru-RU" : "en-US");
    const rate = userLang === "ru" ? 0.95 : 0.97;
    const pitch = 1.03;

    function speakChunk(index) {
      if (sequence !== speechSequence) {
        return;
      }

      if (index >= chunks.length) {
        isSpeaking = false;
        renderActions();
        return;
      }

      const utterance = new SpeechSynthesisUtterance(chunks[index]);
      utterance.lang = speechLang;
      utterance.rate = rate;
      utterance.pitch = pitch;
      if (preferredVoice) {
        utterance.voice = preferredVoice;
      }

      utterance.onend = function onSpeechEnd() {
        speakChunk(index + 1);
      };
      utterance.onerror = function onSpeechError() {
        if (sequence !== speechSequence) {
          return;
        }
        isSpeaking = false;
        state.voiceError = userLang === "ru"
          ? "Не удалось озвучить ответ браузером."
          : "Failed to speak with the browser voice.";
        renderActions();
        renderComposerOnly();
      };

      try {
        synthesis.speak(utterance);
      } catch {
        if (sequence !== speechSequence) {
          return;
        }
        isSpeaking = false;
        state.voiceError = userLang === "ru"
          ? "Не удалось запустить озвучку."
          : "Failed to start speech playback.";
        renderActions();
        renderComposerOnly();
      }
    }

    speakChunk(0);
  }

  function appendMessage(role, text, mentions) {
    const normalized = normalizeText(text);
    if (!normalized) {
      return;
    }

    const normalizedMentions = role === "assistant" ? normalizeMentionList(mentions) : [];

    state.messages.push({
      id: createId(),
      role,
      text: normalized,
      mentions: normalizedMentions,
    });

    renderMessages();
  }

  function render() {
    root.innerHTML = "";

    if (!isOpen) {
      root.appendChild(renderLauncher());
      return;
    }

    const panel = document.createElement("section");
    panel.className = PANEL_CLASS;
    panel.setAttribute("role", "dialog");
    panel.setAttribute("aria-label", t.title);

    panel.appendChild(renderHeader());
    panel.appendChild(renderModeSwitch());
    panel.appendChild(renderMessagesContainer());
    panel.appendChild(renderSuggestions());
    panel.appendChild(renderComposer());

    root.appendChild(panel);
    requestAnimationFrame(scrollToBottom);
  }

  function renderLauncher() {
    const launcher = document.createElement("button");
    launcher.type = "button";
    launcher.className = LAUNCHER_CLASS;
    launcher.setAttribute("aria-label", t.openLabel);
    launcher.innerHTML = `
      <img src="${avatarPath}" alt="Assistant avatar" class="legacy-assistant__launcher-avatar" />
      <span class="legacy-assistant__launcher-label">AI</span>
    `;
    launcher.addEventListener("click", function openPanel() {
      isOpen = true;
      render();
    });

    return launcher;
  }

  function renderHeader() {
    const header = document.createElement("header");
    header.className = "legacy-assistant__header";

    const identity = document.createElement("div");
    identity.className = "legacy-assistant__identity";
    identity.innerHTML = `
      <img src="${avatarPath}" alt="Assistant avatar" class="legacy-assistant__avatar" />
      <div class="legacy-assistant__identity-copy">
        <strong>${escapeHtml(t.title)}</strong>
        <span>${escapeHtml(t.subtitle)}</span>
      </div>
    `;

    const closeBtn = document.createElement("button");
    closeBtn.type = "button";
    closeBtn.className = "legacy-assistant__close-btn";
    closeBtn.setAttribute("aria-label", t.closeLabel);
    closeBtn.textContent = "×";
    closeBtn.addEventListener("click", function closePanel() {
      isOpen = false;
      stopListening();
      stopSpeaking(false);
      render();
    });

    header.appendChild(identity);
    header.appendChild(closeBtn);
    return header;
  }

  function renderModeSwitch() {
    const wrap = document.createElement("div");
    wrap.className = "legacy-assistant__mode-switch";
    wrap.setAttribute("role", "tablist");

    const textBtn = document.createElement("button");
    textBtn.type = "button";
    textBtn.className = `legacy-assistant__mode-btn${mode === "text" ? " is-active" : ""}`;
    textBtn.textContent = t.textMode;
    textBtn.setAttribute("role", "tab");
    textBtn.setAttribute("aria-selected", String(mode === "text"));
    textBtn.addEventListener("click", function setTextMode() {
      mode = "text";
      writeMode(mode);
      stopListening();
      state.voiceError = "";
      render();
    });

    const voiceBtn = document.createElement("button");
    voiceBtn.type = "button";
    voiceBtn.className = `legacy-assistant__mode-btn${mode === "voice" ? " is-active" : ""}`;
    voiceBtn.textContent = t.voiceMode;
    voiceBtn.setAttribute("role", "tab");
    voiceBtn.setAttribute("aria-selected", String(mode === "voice"));
    voiceBtn.disabled = !speechRecognitionCtor;
    voiceBtn.addEventListener("click", function setVoiceMode() {
      if (!speechRecognitionCtor) {
        state.voiceError = t.voiceUnsupported;
        renderComposer();
        return;
      }
      mode = "voice";
      writeMode(mode);
      render();
    });

    wrap.appendChild(textBtn);
    wrap.appendChild(voiceBtn);
    return wrap;
  }

  function renderMessagesContainer() {
    const container = document.createElement("div");
    container.className = "legacy-assistant__messages";
    container.setAttribute("aria-label", t.chatHistoryLabel);

    state.messages.forEach(function eachMessage(message) {
      const article = document.createElement("article");
      article.className = `legacy-assistant__message legacy-assistant__message--${message.role}`;

      const p = document.createElement("p");
      const parts = message.role === "assistant" ? splitMessageByMentions(message.text, message.mentions) : [{ type: "text", text: message.text }];
      parts.forEach(function eachPart(part, partIndex) {
        if (part.type === "mention" && part.mention) {
          const mentionButton = document.createElement("button");
          mentionButton.type = "button";
          mentionButton.className = "legacy-assistant__client-link";
          mentionButton.textContent = part.text;
          mentionButton.title = userLang === "ru" ? "Открыть карточку клиента" : "Open client card";
          mentionButton.addEventListener("click", function onMentionClick() {
            dispatchOpenClientEvent(part.mention || part.text);
          });
          p.appendChild(mentionButton);
          return;
        }

        const span = document.createElement("span");
        span.textContent = part.text;
        span.setAttribute("data-part-index", String(partIndex));
        p.appendChild(span);
      });
      article.appendChild(p);

      container.appendChild(article);
    });

    if (isSending) {
      const pending = document.createElement("article");
      pending.className = "legacy-assistant__message legacy-assistant__message--assistant legacy-assistant__message--pending";
      const p = document.createElement("p");
      p.textContent = t.thinking;
      pending.appendChild(p);
      container.appendChild(pending);
    }

    const end = document.createElement("div");
    end.className = "legacy-assistant__messages-end";
    container.appendChild(end);

    return container;
  }

  function renderMessages() {
    const container = root.querySelector(".legacy-assistant__messages");
    if (!container) {
      return;
    }

    const next = renderMessagesContainer();
    container.replaceWith(next);
    scrollToBottom();
  }

  function renderSuggestions() {
    const wrap = document.createElement("div");
    wrap.className = "legacy-assistant__suggestions";
    wrap.setAttribute("aria-label", t.suggestionsLabel);

    const chips = state.suggestions.slice(0, 4);
    chips.forEach(function eachSuggestion(item) {
      const btn = document.createElement("button");
      btn.type = "button";
      btn.className = "legacy-assistant__suggestion-chip";
      btn.textContent = item;
      btn.disabled = isSending;
      btn.addEventListener("click", function sendSuggestion() {
        void sendMessage(item, true);
      });
      wrap.appendChild(btn);
    });

    return wrap;
  }

  function renderSuggestionsOnly() {
    const old = root.querySelector(".legacy-assistant__suggestions");
    if (!old) {
      return;
    }
    old.replaceWith(renderSuggestions());
  }

  function renderComposer() {
    const form = document.createElement("form");
    form.className = "legacy-assistant__composer";

    const textarea = document.createElement("textarea");
    textarea.placeholder = t.inputPlaceholder;
    textarea.rows = 3;
    textarea.maxLength = MAX_TEXT_LENGTH;
    textarea.disabled = isSending;
    textarea.addEventListener("keydown", function onKeyDown(event) {
      if (event.key === "Enter" && !event.shiftKey) {
        event.preventDefault();
        void sendMessage(textarea.value, false);
      }
    });

    form.appendChild(textarea);

    const actions = document.createElement("div");
    actions.className = "legacy-assistant__actions";

    if (mode === "voice") {
      const micBtn = document.createElement("button");
      micBtn.type = "button";
      micBtn.className = `legacy-assistant__action-btn${isListening ? " is-active" : ""}`;
      micBtn.disabled = isSending || !speechRecognitionCtor;
      micBtn.textContent = isListening ? t.stopMic : t.startMic;
      micBtn.addEventListener("click", function onMicClick() {
        if (isListening) {
          stopListening();
        } else {
          startListening(textarea);
        }
      });
      actions.appendChild(micBtn);

      const speakBtn = document.createElement("button");
      speakBtn.type = "button";
      speakBtn.className = `legacy-assistant__action-btn${isSpeaking ? " is-active" : ""}`;
      speakBtn.disabled = isSending;
      speakBtn.textContent = isSpeaking ? t.stopSpeak : t.speakReply;
      speakBtn.addEventListener("click", function onSpeakClick() {
        if (isSpeaking) {
          stopSpeaking();
          return;
        }

        const lastAssistantMessage = [...state.messages].reverse().find(function findAssistant(msg) {
          return msg.role === "assistant";
        });
        if (lastAssistantMessage) {
          speakText(lastAssistantMessage.text);
        }
      });
      actions.appendChild(speakBtn);
    }

    const sendBtn = document.createElement("button");
    sendBtn.type = "submit";
    sendBtn.className = "legacy-assistant__send-btn";
    sendBtn.textContent = isSending ? t.sending : t.send;
    sendBtn.disabled = isSending;

    actions.appendChild(sendBtn);
    form.appendChild(actions);

    if (state.voiceError) {
      const error = document.createElement("p");
      error.className = "legacy-assistant__voice-error";
      error.textContent = state.voiceError;
      form.appendChild(error);
    }

    form.addEventListener("submit", function onSubmit(event) {
      event.preventDefault();
      void sendMessage(textarea.value, false);
    });

    return form;
  }

  function renderComposerOnly() {
    const old = root.querySelector(".legacy-assistant__composer");
    if (!old) {
      return;
    }
    old.replaceWith(renderComposer());
  }

  function renderActions() {
    renderComposerOnly();
  }

  function escapeHtml(value) {
    return (value || "")
      .toString()
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/\"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  function scrollToBottom() {
    const messages = root.querySelector(".legacy-assistant__messages");
    if (!messages) {
      return;
    }

    messages.scrollTop = messages.scrollHeight;
  }

  function getVoiceErrorText(errorCode) {
    if (errorCode === "not-allowed") {
      return t.voiceNotAllowed;
    }
    if (errorCode === "no-speech") {
      return t.voiceNoSpeech;
    }
    return t.voiceFailed;
  }

  function startListening(textarea) {
    state.voiceError = "";
    renderComposerOnly();

    if (!speechRecognitionCtor) {
      state.voiceError = t.voiceUnsupported;
      renderComposerOnly();
      return;
    }

    stopListening(true);

    try {
      speechRecognition = new speechRecognitionCtor();
      speechRecognition.lang = userLang === "ru" ? "ru-RU" : "en-US";
      speechRecognition.interimResults = false;
      speechRecognition.continuous = false;
      speechRecognition.maxAlternatives = 1;

      speechRecognition.onstart = function onStart() {
        isListening = true;
        renderActions();
      };

      speechRecognition.onend = function onEnd() {
        isListening = false;
        speechRecognition = null;
        renderActions();
      };

      speechRecognition.onerror = function onError(event) {
        state.voiceError = getVoiceErrorText((event && event.error) || "");
        isListening = false;
        renderActions();
      };

      speechRecognition.onresult = function onResult(event) {
        let transcript = "";

        for (let index = 0; index < event.results.length; index += 1) {
          const result = event.results[index];
          if (!result || !result[0]) {
            continue;
          }
          transcript += ` ${result[0].transcript || ""}`;
        }

        const normalized = normalizeText(transcript);
        if (normalized) {
          const current = normalizeText(textarea.value);
          textarea.value = current ? `${current} ${normalized}` : normalized;
          textarea.focus();
        }
      };

      speechRecognition.start();
    } catch {
      state.voiceError = t.micStartFailed;
      isListening = false;
      renderActions();
    }
  }

  function stopListening(silent) {
    if (!speechRecognition) {
      isListening = false;
      if (!silent) {
        renderActions();
      }
      return;
    }

    try {
      speechRecognition.stop();
    } catch {
      // ignore
    }

    speechRecognition = null;
    isListening = false;
    if (!silent) {
      renderActions();
    }
  }

  async function sendMessage(rawText, keepDraft) {
    const text = normalizeText(rawText);
    if (!text || isSending) {
      return;
    }

    const textarea = root.querySelector(".legacy-assistant__composer textarea");
    if (!keepDraft && textarea) {
      textarea.value = "";
    }

    appendMessage("user", text);
    isSending = true;
    renderMessages();
    renderSuggestionsOnly();
    renderComposerOnly();

    try {
      const response = await fetch(API_PATH, {
        method: "POST",
        credentials: "same-origin",
        headers: {
          "Content-Type": "application/json",
          Accept: "application/json",
        },
        body: JSON.stringify({
          message: text,
          mode: mode === "voice" ? "voice" : "text",
        }),
      });

      if (!response.ok) {
        let message = t.networkError;
        try {
          const payload = await response.json();
          if (payload && typeof payload.error === "string" && payload.error) {
            message = payload.error;
          }
        } catch {
          // ignore JSON parse errors
        }

        if (response.status === 401) {
          const nextPath = `${window.location.pathname}${window.location.search}`;
          window.location.assign(`/login?next=${encodeURIComponent(nextPath)}`);
          return;
        }

        appendMessage("assistant", message);
        return;
      }

      let payload = null;
      try {
        payload = await response.json();
      } catch {
        payload = null;
      }

      const reply = normalizeText(payload && payload.reply ? payload.reply : "");
      const mentions = normalizeMentionList(payload && Array.isArray(payload.clientMentions) ? payload.clientMentions : []);
      if (reply) {
        appendMessage("assistant", reply, mentions);
        if (mode === "voice") {
          speakText(reply);
        }
      }

      if (payload && Array.isArray(payload.suggestions) && payload.suggestions.length) {
        state.suggestions = payload.suggestions.slice(0, 8).map(function normalizeSuggestion(item) {
          return normalizeText(item);
        }).filter(Boolean);
      }
    } catch {
      appendMessage("assistant", t.networkError);
    } finally {
      isSending = false;
      renderMessages();
      renderSuggestionsOnly();
      renderComposerOnly();
    }
  }
})();
