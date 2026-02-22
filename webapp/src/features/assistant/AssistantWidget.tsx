import { useEffect, useMemo, useRef, useState } from "react";

import assistantAvatar from "@/assets/assistant-avatar.svg";
import { ApiError, queueAssistantSessionContextResetBeacon, resetAssistantSessionContext, sendAssistantMessage } from "@/shared/api";
import { cx } from "@/shared/lib/cx";
import type { AssistantMode } from "@/shared/types/assistant";

interface ChatMessage {
  id: string;
  role: "assistant" | "user";
  text: string;
  mentions: string[];
  createdAt: number;
}

interface MessagePart {
  type: "text" | "mention";
  text: string;
  mention?: string;
}

interface SpeechRecognitionAlternativeLike {
  transcript: string;
}

interface SpeechRecognitionResultLike {
  isFinal: boolean;
  length: number;
  [index: number]: SpeechRecognitionAlternativeLike;
}

interface SpeechRecognitionResultListLike {
  length: number;
  [index: number]: SpeechRecognitionResultLike;
}

interface SpeechRecognitionEventLike extends Event {
  resultIndex: number;
  results: SpeechRecognitionResultListLike;
}

interface SpeechRecognitionErrorEventLike extends Event {
  error: string;
}

interface SpeechRecognitionLike extends EventTarget {
  continuous: boolean;
  interimResults: boolean;
  lang: string;
  maxAlternatives: number;
  onstart: (() => void) | null;
  onend: (() => void) | null;
  onerror: ((event: SpeechRecognitionErrorEventLike) => void) | null;
  onresult: ((event: SpeechRecognitionEventLike) => void) | null;
  start(): void;
  stop(): void;
  abort(): void;
}

interface SpeechRecognitionConstructorLike {
  new (): SpeechRecognitionLike;
}

interface WindowWithSpeechRecognition extends Window {
  SpeechRecognition?: SpeechRecognitionConstructorLike;
  webkitSpeechRecognition?: SpeechRecognitionConstructorLike;
}

const DEFAULT_SUGGESTIONS_EN = [
  "Give me a client summary",
  "Show top 10 debtors",
  "How many new clients from 2026-02-01 to 2026-02-09?",
  "How many first payments in the last 30 days?",
  "Revenue by week for the last 2 months",
  "Who stopped paying after 2025-10-01?",
  "Manager ranking by debt",
  "Show client John Smith",
];

const DEFAULT_SUGGESTIONS_RU = [
  "Сводка по клиентам",
  "Покажи топ-10 должников",
  "Сколько новых клиентов с 2026-02-01 по 2026-02-09?",
  "Сколько первых платежей за последние 30 дней?",
  "Выручка по неделям за последние 2 месяца",
  "Кто перестал платить после 2025-10-01?",
  "Рейтинг менеджеров по долгу",
  "Покажи клиента John Smith",
];
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
const ASSISTANT_CHAT_SESSION_STORAGE_KEY = "cbooster_assistant_chat_session_id";
const ASSISTANT_CONTEXT_RESET_MAX_ATTEMPTS = 2;
const ASSISTANT_CONTEXT_RESET_RETRY_DELAY_MS = 350;
const ASSISTANT_CONTEXT_RESET_TIMEOUT_MS = 4_000;

function resolveSpeechRecognitionConstructor(): SpeechRecognitionConstructorLike | null {
  if (typeof window === "undefined") {
    return null;
  }

  const windowWithSpeech = window as WindowWithSpeechRecognition;
  return windowWithSpeech.SpeechRecognition || windowWithSpeech.webkitSpeechRecognition || null;
}

function delayMs(durationMs: number): Promise<void> {
  return new Promise((resolve) => {
    globalThis.setTimeout(resolve, Math.max(0, durationMs));
  });
}

function emitAssistantContextResetFailureMetric(
  stage: "keepalive_retry_exhausted" | "beacon_failed",
  sessionId: string,
  error?: unknown,
): void {
  const reason =
    error instanceof Error
      ? error.message
      : typeof error === "string"
        ? error
        : "unknown_error";
  const detail = {
    stage,
    sessionId,
    reason,
    timestamp: new Date().toISOString(),
  };

  if (typeof window !== "undefined") {
    window.dispatchEvent(
      new CustomEvent("cb-assistant-context-reset-failure", {
        detail,
      }),
    );
  }

  console.warn("[assistant.context_reset_failure]", detail);
}

function generateMessageId(): string {
  return `${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
}

function createAssistantGreetingMessage(isRussian: boolean): ChatMessage {
  return {
    id: generateMessageId(),
    role: "assistant",
    text: isRussian
      ? "Привет. Я могу ответить по клиентским данным: сводки, топы, просрочки, рейтинги менеджеров, динамика по периодам и карточки клиентов."
      : "Hi. I can answer from client data: summaries, top lists, overdue filters, manager rankings, period analytics, and client cards.",
    mentions: [],
    createdAt: Date.now(),
  };
}

function generateAssistantSessionId(): string {
  if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") {
    return crypto.randomUUID();
  }
  return `assistant_${Date.now()}_${Math.random().toString(36).slice(2, 10)}`;
}

function persistAssistantSessionId(sessionId: string): void {
  if (typeof window === "undefined") {
    return;
  }

  try {
    window.localStorage.setItem(ASSISTANT_CHAT_SESSION_STORAGE_KEY, sessionId);
  } catch {
    // Ignore localStorage write failures.
  }
}

function resolveAssistantSessionId(): string {
  if (typeof window === "undefined") {
    return generateAssistantSessionId();
  }

  try {
    const stored = normalizeOutgoingMessage(window.localStorage.getItem(ASSISTANT_CHAT_SESSION_STORAGE_KEY) || "");
    if (/^[a-z0-9_.:-]{8,120}$/i.test(stored)) {
      return stored;
    }
  } catch {
    // Ignore localStorage access failures and use in-memory id.
  }

  const nextSessionId = generateAssistantSessionId();
  persistAssistantSessionId(nextSessionId);
  return nextSessionId;
}

function normalizeOutgoingMessage(rawValue: string): string {
  return rawValue.replace(/\s+/g, " ").trim();
}

function normalizeDisplayMessage(rawValue: string): string {
  return rawValue.trim();
}

function extractSpeechTranscript(results: SpeechRecognitionResultListLike): string {
  const chunks: string[] = [];

  for (let index = 0; index < results.length; index += 1) {
    const result = results[index];
    if (!result || result.length < 1) {
      continue;
    }

    const bestAlternative = result[0];
    const transcript = normalizeOutgoingMessage(bestAlternative?.transcript || "");
    if (transcript) {
      chunks.push(transcript);
    }
  }

  return normalizeOutgoingMessage(chunks.join(" "));
}

function formatVoiceError(errorCode: string, isRussian: boolean): string {
  if (isRussian) {
    if (errorCode === "not-allowed") {
      return "Доступ к микрофону запрещен браузером.";
    }
    if (errorCode === "no-speech") {
      return "Голос не распознан. Попробуйте еще раз.";
    }
    return "Не удалось обработать голосовой ввод.";
  }

  if (errorCode === "not-allowed") {
    return "Microphone permission is blocked by the browser.";
  }
  if (errorCode === "no-speech") {
    return "No speech detected. Please try again.";
  }
  return "Unable to process voice input.";
}

function resolveUserLanguage(): "ru" | "en" {
  if (typeof navigator === "undefined") {
    return "en";
  }

  return /^ru\b/i.test(navigator.language || "") ? "ru" : "en";
}

function normalizeMentionList(rawMentions: string[] | undefined): string[] {
  if (!Array.isArray(rawMentions)) {
    return [];
  }

  const seen = new Set<string>();
  const mentions: string[] = [];

  for (const rawMention of rawMentions) {
    const mention = normalizeDisplayMessage(rawMention);
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

  return mentions.sort((left, right) => right.length - left.length);
}

function splitMessageByMentions(text: string, mentions: string[]): MessagePart[] {
  const normalizedText = text || "";
  const mentionList = normalizeMentionList(mentions);
  if (!mentionList.length) {
    return [{ type: "text", text: normalizedText }];
  }

  const lowerText = normalizedText.toLowerCase();
  const parts: MessagePart[] = [];
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

  return parts.filter((part) => part.text.length > 0);
}

function scoreSpeechVoice(voice: SpeechSynthesisVoice, isRussian: boolean): number {
  const name = `${voice.name || ""} ${voice.voiceURI || ""}`.toLowerCase();
  const lang = (voice.lang || "").toLowerCase();
  let score = 0;

  if (isRussian) {
    if (lang.startsWith("ru")) {
      score += 100;
    }
    if (lang.startsWith("ru-ru")) {
      score += 16;
    }
  } else {
    if (lang.startsWith("en")) {
      score += 100;
    }
    if (lang.startsWith("en-us")) {
      score += 16;
    }
  }

  if (FEMALE_VOICE_HINTS.some((hint) => name.includes(hint))) {
    score += 42;
  }
  if (MALE_VOICE_HINTS.some((hint) => name.includes(hint))) {
    score -= 30;
  }
  if (NATURAL_VOICE_HINTS.some((hint) => name.includes(hint))) {
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

function pickPreferredSpeechVoice(voices: SpeechSynthesisVoice[], isRussian: boolean): SpeechSynthesisVoice | null {
  if (!voices.length) {
    return null;
  }

  let bestVoice: SpeechSynthesisVoice | null = null;
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

function splitSpeechIntoChunks(rawText: string, maxLength = 220): string[] {
  const normalized = rawText.replace(/\s+/g, " ").trim();
  if (!normalized) {
    return [];
  }

  const sentenceCandidates = normalized
    .replace(/([.!?;:])\s+/g, "$1\n")
    .split(/\n+/)
    .map((item) => item.trim())
    .filter(Boolean);

  const chunks: string[] = [];
  let current = "";

  const pushValue = (value: string): void => {
    const text = value.trim();
    if (!text) {
      return;
    }
    if (text.length <= maxLength) {
      chunks.push(text);
      return;
    }

    const commaParts = text
      .split(/,\s+/)
      .map((part) => part.trim())
      .filter(Boolean);

    if (commaParts.length > 1) {
      for (const part of commaParts) {
        if (part.length <= maxLength) {
          chunks.push(part);
          continue;
        }
        const words = part.split(/\s+/);
        let buffer = "";
        for (const word of words) {
          const candidate = buffer ? `${buffer} ${word}` : word;
          if (candidate.length > maxLength) {
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
  };

  for (const sentence of sentenceCandidates) {
    const candidate = current ? `${current} ${sentence}` : sentence;
    if (candidate.length > maxLength) {
      if (current) {
        pushValue(current);
      }
      current = sentence;
      continue;
    }
    current = candidate;
  }

  if (current) {
    pushValue(current);
  }

  return chunks;
}

function resolvePreferredSpeechVoice(isRussian: boolean): Promise<SpeechSynthesisVoice | null> {
  if (typeof window === "undefined" || !("speechSynthesis" in window)) {
    return Promise.resolve(null);
  }

  const synthesis = window.speechSynthesis;
  const immediateVoice = pickPreferredSpeechVoice(synthesis.getVoices(), isRussian);
  if (immediateVoice) {
    return Promise.resolve(immediateVoice);
  }

  return new Promise((resolve) => {
    let settled = false;
    const settle = () => {
      if (settled) {
        return;
      }
      settled = true;
      synthesis.removeEventListener("voiceschanged", handleVoicesChanged);
      clearTimeout(timeoutId);
      resolve(pickPreferredSpeechVoice(synthesis.getVoices(), isRussian));
    };
    const handleVoicesChanged = () => {
      settle();
    };
    const timeoutId = window.setTimeout(settle, 1200);
    synthesis.addEventListener("voiceschanged", handleVoicesChanged);
  });
}

export function AssistantWidget() {
  const language = useMemo(() => resolveUserLanguage(), []);
  const isRussian = language === "ru";
  const defaultSuggestions = isRussian ? DEFAULT_SUGGESTIONS_RU : DEFAULT_SUGGESTIONS_EN;
  const speechRecognitionSupported = useMemo(() => resolveSpeechRecognitionConstructor() !== null, []);

  const [isOpen, setIsOpen] = useState(false);
  const [mode, setMode] = useState<AssistantMode>("text");
  const [draft, setDraft] = useState("");
  const [messages, setMessages] = useState<ChatMessage[]>(() => [createAssistantGreetingMessage(isRussian)]);
  const [suggestions, setSuggestions] = useState<string[]>(defaultSuggestions);
  const [isSending, setIsSending] = useState(false);
  const [isListening, setIsListening] = useState(false);
  const [isSpeaking, setIsSpeaking] = useState(false);
  const [voiceError, setVoiceError] = useState("");

  const isMountedRef = useRef(true);
  const recognitionRef = useRef<SpeechRecognitionLike | null>(null);
  const abortControllerRef = useRef<AbortController | null>(null);
  const speechSequenceRef = useRef(0);
  const messagesEndRef = useRef<HTMLDivElement | null>(null);
  const chatSessionIdRef = useRef<string>(resolveAssistantSessionId());

  const hasSendableText = normalizeOutgoingMessage(draft).length > 0;

  function renewAssistantSessionId(): void {
    const nextSessionId = generateAssistantSessionId();
    chatSessionIdRef.current = nextSessionId;
    persistAssistantSessionId(nextSessionId);
  }

  async function resetAssistantServerContextOnClose(sessionId: string): Promise<void> {
    let lastError: unknown;

    for (let attempt = 1; attempt <= ASSISTANT_CONTEXT_RESET_MAX_ATTEMPTS; attempt += 1) {
      try {
        await resetAssistantSessionContext(sessionId, {
          keepalive: true,
          timeoutMs: ASSISTANT_CONTEXT_RESET_TIMEOUT_MS,
        });
        return;
      } catch (error) {
        lastError = error;
      }

      if (attempt < ASSISTANT_CONTEXT_RESET_MAX_ATTEMPTS) {
        await delayMs(ASSISTANT_CONTEXT_RESET_RETRY_DELAY_MS * attempt);
      }
    }

    emitAssistantContextResetFailureMetric("keepalive_retry_exhausted", sessionId, lastError);

    if (!queueAssistantSessionContextResetBeacon(sessionId)) {
      emitAssistantContextResetFailureMetric("beacon_failed", sessionId, lastError);
    }
  }

  function resetAssistantConversation(clearServerContext = true): void {
    const currentSessionId = chatSessionIdRef.current;

    abortControllerRef.current?.abort();
    abortControllerRef.current = null;
    stopListening();
    stopSpeaking();

    if (clearServerContext) {
      void resetAssistantServerContextOnClose(currentSessionId);
    }

    setDraft("");
    setMessages([createAssistantGreetingMessage(isRussian)]);
    setSuggestions(defaultSuggestions);
    setMode("text");
    setIsSending(false);
    setVoiceError("");
    renewAssistantSessionId();
  }

  useEffect(() => {
    if (!speechRecognitionSupported && mode === "voice") {
      setMode("text");
    }
  }, [mode, speechRecognitionSupported]);

  useEffect(() => {
    if (!isOpen) {
      return;
    }

    messagesEndRef.current?.scrollIntoView({ behavior: "smooth", block: "end" });
  }, [isOpen, messages]);

  useEffect(() => {
    return () => {
      isMountedRef.current = false;
      abortControllerRef.current?.abort();
      recognitionRef.current?.abort();
      stopSpeaking(false);
    };
  }, []);

  function appendMessage(role: ChatMessage["role"], text: string, mentions: string[] = []): void {
    const normalizedText = normalizeDisplayMessage(text);
    if (!normalizedText) {
      return;
    }

    const normalizedMentions = role === "assistant" ? normalizeMentionList(mentions) : [];

    setMessages((previous) => [
      ...previous,
      {
        id: generateMessageId(),
        role,
        text: normalizedText,
        mentions: normalizedMentions,
        createdAt: Date.now(),
      },
    ]);
  }

  function emitOpenClientEvent(clientName: string): void {
    const normalizedClientName = normalizeDisplayMessage(clientName);
    if (!normalizedClientName || typeof window === "undefined") {
      return;
    }

    window.dispatchEvent(
      new CustomEvent("cb-assistant-open-client", {
        detail: {
          clientName: normalizedClientName,
        },
      }),
    );
  }

  function stopSpeaking(resetSpeaking = true): void {
    speechSequenceRef.current += 1;
    if (typeof window !== "undefined" && "speechSynthesis" in window) {
      window.speechSynthesis.cancel();
    }
    if (resetSpeaking && isMountedRef.current) {
      setIsSpeaking(false);
    }
  }

  async function speakTextAsync(rawText: string): Promise<void> {
    const text = normalizeDisplayMessage(rawText);
    if (!text) {
      return;
    }

    if (typeof window === "undefined" || !("speechSynthesis" in window)) {
      setVoiceError(isRussian ? "Ваш браузер не поддерживает голосовой синтез." : "Your browser does not support speech synthesis.");
      return;
    }

    stopSpeaking(false);
    setVoiceError("");
    if (isMountedRef.current) {
      setIsSpeaking(true);
    }

    const sequence = speechSequenceRef.current + 1;
    speechSequenceRef.current = sequence;
    const chunks = splitSpeechIntoChunks(text);
    if (!chunks.length) {
      setIsSpeaking(false);
      return;
    }

    const synthesis = window.speechSynthesis;
    const preferredVoice = await resolvePreferredSpeechVoice(isRussian);
    if (!isMountedRef.current || sequence !== speechSequenceRef.current) {
      return;
    }

    const language = preferredVoice?.lang || (isRussian ? "ru-RU" : "en-US");
    const rate = isRussian ? 0.95 : 0.97;
    const pitch = 1.03;

    const speakChunk = (chunkIndex: number): void => {
      if (sequence !== speechSequenceRef.current || !isMountedRef.current) {
        return;
      }

      if (chunkIndex >= chunks.length) {
        setIsSpeaking(false);
        return;
      }

      const utterance = new SpeechSynthesisUtterance(chunks[chunkIndex]);
      utterance.lang = language;
      utterance.rate = rate;
      utterance.pitch = pitch;
      if (preferredVoice) {
        utterance.voice = preferredVoice;
      }
      utterance.onend = () => {
        speakChunk(chunkIndex + 1);
      };
      utterance.onerror = () => {
        if (sequence !== speechSequenceRef.current || !isMountedRef.current) {
          return;
        }
        setIsSpeaking(false);
        setVoiceError(isRussian ? "Не удалось озвучить ответ браузером." : "Failed to speak with the browser voice.");
      };

      try {
        synthesis.speak(utterance);
      } catch {
        if (sequence !== speechSequenceRef.current || !isMountedRef.current) {
          return;
        }
        setIsSpeaking(false);
        setVoiceError(isRussian ? "Не удалось запустить озвучку." : "Failed to start speech playback.");
      }
    };

    speakChunk(0);
  }

  function speakText(rawText: string): void {
    void speakTextAsync(rawText);
  }

  function stopListening(): void {
    recognitionRef.current?.stop();
  }

  function startListening(): void {
    setVoiceError("");

    const RecognitionCtor = resolveSpeechRecognitionConstructor();
    if (!RecognitionCtor) {
      setVoiceError(isRussian ? "Ваш браузер не поддерживает голосовой ввод." : "Your browser does not support voice input.");
      return;
    }

    recognitionRef.current?.abort();

    try {
      const recognition = new RecognitionCtor();
      recognition.lang = isRussian ? "ru-RU" : "en-US";
      recognition.interimResults = false;
      recognition.continuous = false;
      recognition.maxAlternatives = 1;
      recognition.onstart = () => {
        if (isMountedRef.current) {
          setIsListening(true);
        }
      };
      recognition.onend = () => {
        if (isMountedRef.current) {
          setIsListening(false);
        }
        recognitionRef.current = null;
      };
      recognition.onerror = (event) => {
        if (isMountedRef.current) {
          setVoiceError(formatVoiceError(event.error || "", isRussian));
          setIsListening(false);
        }
      };
      recognition.onresult = (event) => {
        const transcript = extractSpeechTranscript(event.results);
        if (!transcript || !isMountedRef.current) {
          return;
        }

        setDraft((previous) => {
          const previousText = normalizeOutgoingMessage(previous);
          return previousText ? `${previousText} ${transcript}` : transcript;
        });
      };

      recognitionRef.current = recognition;
      recognition.start();
    } catch {
      setVoiceError(isRussian ? "Не удалось включить микрофон." : "Failed to start microphone.");
      setIsListening(false);
    }
  }

  async function handleSend(rawText?: string): Promise<void> {
    const outgoingText = normalizeOutgoingMessage(rawText ?? draft);
    if (!outgoingText || isSending) {
      return;
    }

    if (rawText === undefined) {
      setDraft("");
    }

    appendMessage("user", outgoingText);
    setIsSending(true);
    setVoiceError("");

    abortControllerRef.current?.abort();
    const controller = new AbortController();
    abortControllerRef.current = controller;

    try {
      const response = await sendAssistantMessage(outgoingText, mode, controller.signal, chatSessionIdRef.current);
      const replyText = normalizeDisplayMessage(response.reply || "");

      if (replyText) {
        appendMessage("assistant", replyText, response.clientMentions || []);
        if (mode === "voice") {
          speakText(replyText);
        }
      }

      if (Array.isArray(response.suggestions) && response.suggestions.length) {
        setSuggestions(response.suggestions.slice(0, 8));
      }
    } catch (error) {
      if (error instanceof ApiError && error.code === "aborted") {
        return;
      }

      const fallbackMessage =
        error instanceof Error && error.message
          ? error.message
          : isRussian
            ? "Не удалось получить ответ ассистента."
            : "Failed to receive an assistant response.";

      appendMessage(
        "assistant",
        isRussian
          ? `Техническая ошибка: ${fallbackMessage}`
          : `Technical error: ${fallbackMessage}`,
      );
    } finally {
      if (isMountedRef.current) {
        setIsSending(false);
      }
      if (abortControllerRef.current === controller) {
        abortControllerRef.current = null;
      }
    }
  }

  function handleFormSubmit(event: React.FormEvent<HTMLFormElement>): void {
    event.preventDefault();
    void handleSend();
  }

  function handleTextAreaKeyDown(event: React.KeyboardEvent<HTMLTextAreaElement>): void {
    if (event.key === "Enter" && !event.shiftKey) {
      event.preventDefault();
      void handleSend();
    }
  }

  function handleToggleListening(): void {
    if (isListening) {
      stopListening();
      return;
    }
    startListening();
  }

  function handleSuggestionClick(value: string): void {
    if (isSending) {
      return;
    }
    void handleSend(value);
  }

  function handleSpeakLastReply(): void {
    if (isSpeaking) {
      stopSpeaking();
      return;
    }

    const lastAssistantMessage = [...messages].reverse().find((message) => message.role === "assistant");
    if (!lastAssistantMessage) {
      return;
    }

    speakText(lastAssistantMessage.text);
  }

  function renderMessageParts(message: ChatMessage) {
    if (message.role !== "assistant" || !message.mentions.length) {
      return message.text;
    }

    const parts = splitMessageByMentions(message.text, message.mentions);
    return parts.map((part, index) => {
      if (part.type !== "mention" || !part.mention) {
        return <span key={`${message.id}-text-${index}`}>{part.text}</span>;
      }

      return (
        <button
          key={`${message.id}-mention-${index}`}
          type="button"
          className="assistant-widget__client-link"
          onClick={() => emitOpenClientEvent(part.mention || part.text)}
          title={isRussian ? "Открыть карточку клиента" : "Open client card"}
        >
          {part.text}
        </button>
      );
    });
  }

  return (
    <div className="assistant-widget" aria-live="polite">
      {!isOpen ? (
        <button
          type="button"
          className="assistant-widget__launcher"
          aria-label={isRussian ? "Открыть помощника" : "Open assistant"}
          onClick={() => setIsOpen(true)}
        >
          <img src={assistantAvatar} alt="Assistant avatar" className="assistant-widget__launcher-avatar" />
          <span className="assistant-widget__launcher-label">AI</span>
        </button>
      ) : null}

      {isOpen ? (
        <section className="assistant-widget__panel" role="dialog" aria-label={isRussian ? "Онлайн помощник" : "Online assistant"}>
          <header className="assistant-widget__header">
            <div className="assistant-widget__identity">
              <img src={assistantAvatar} alt="Assistant avatar" className="assistant-widget__avatar" />
              <div className="assistant-widget__identity-copy">
                <strong>{isRussian ? "Credit Booster Помощник" : "Credit Booster Assistant"}</strong>
                <span>{isRussian ? "Внутренние данные клиентов" : "Internal client data"}</span>
              </div>
            </div>
            <button
              type="button"
              className="assistant-widget__close-btn"
              aria-label={isRussian ? "Закрыть" : "Close"}
              onClick={() => {
                setIsOpen(false);
                resetAssistantConversation(true);
              }}
            >
              ×
            </button>
          </header>

          <div className="assistant-widget__mode-switch" role="tablist" aria-label={isRussian ? "Режим общения" : "Assistant mode"}>
            <button
              type="button"
              className={cx("assistant-widget__mode-btn", mode === "text" && "is-active")}
              role="tab"
              aria-selected={mode === "text"}
              onClick={() => setMode("text")}
            >
              {isRussian ? "Текст" : "Text"}
            </button>
            <button
              type="button"
              className={cx("assistant-widget__mode-btn", mode === "gpt" && "is-active")}
              role="tab"
              aria-selected={mode === "gpt"}
              onClick={() => setMode("gpt")}
            >
              GPT
            </button>
            <button
              type="button"
              className={cx("assistant-widget__mode-btn", mode === "voice" && "is-active")}
              role="tab"
              aria-selected={mode === "voice"}
              disabled={!speechRecognitionSupported}
              onClick={() => setMode("voice")}
            >
              {isRussian ? "Голос" : "Voice"}
            </button>
          </div>

          <div className="assistant-widget__messages" aria-label={isRussian ? "История сообщений" : "Chat messages"}>
            {messages.map((message) => (
              <article
                key={message.id}
                className={cx(
                  "assistant-widget__message",
                  message.role === "assistant" ? "assistant-widget__message--assistant" : "assistant-widget__message--user",
                )}
              >
                <p>{renderMessageParts(message)}</p>
              </article>
            ))}
            {isSending ? (
              <article className="assistant-widget__message assistant-widget__message--assistant assistant-widget__message--pending">
                <p>{isRussian ? "Думаю..." : "Thinking..."}</p>
              </article>
            ) : null}
            <div ref={messagesEndRef} />
          </div>

          {suggestions.length ? (
            <div className="assistant-widget__suggestions" aria-label={isRussian ? "Подсказки" : "Suggestions"}>
              {suggestions.slice(0, 4).map((suggestion) => (
                <button
                  key={suggestion}
                  type="button"
                  className="assistant-widget__suggestion-chip"
                  onClick={() => handleSuggestionClick(suggestion)}
                  disabled={isSending}
                >
                  {suggestion}
                </button>
              ))}
            </div>
          ) : null}

          <form className="assistant-widget__composer" onSubmit={handleFormSubmit}>
            <textarea
              value={draft}
              onChange={(event) => setDraft(event.target.value)}
              onKeyDown={handleTextAreaKeyDown}
              placeholder={
                isRussian
                  ? "Например: Покажи топ-5 должников"
                  : "For example: Show top 5 debtors"
              }
              rows={3}
              disabled={isSending}
            />

            <div className="assistant-widget__actions">
              {mode === "voice" ? (
                <button
                  type="button"
                  className={cx("assistant-widget__action-btn", isListening && "is-active")}
                  onClick={handleToggleListening}
                  disabled={isSending || !speechRecognitionSupported}
                >
                  {isListening
                    ? isRussian
                      ? "Остановить микрофон"
                      : "Stop Mic"
                    : isRussian
                      ? "Включить микрофон"
                      : "Start Mic"}
                </button>
              ) : null}

              {mode === "voice" ? (
                <button
                  type="button"
                  className={cx("assistant-widget__action-btn", isSpeaking && "is-active")}
                  onClick={handleSpeakLastReply}
                  disabled={isSending}
                >
                  {isSpeaking
                    ? isRussian
                      ? "Остановить"
                      : "Stop"
                    : isRussian
                      ? "Озвучить ответ"
                      : "Speak Reply"}
                </button>
              ) : null}

              <button
                type="submit"
                className="assistant-widget__send-btn"
                disabled={isSending || !hasSendableText}
              >
                {isSending ? (isRussian ? "Отправка..." : "Sending...") : isRussian ? "Отправить" : "Send"}
              </button>
            </div>

            {voiceError ? <p className="assistant-widget__voice-error">{voiceError}</p> : null}
          </form>
        </section>
      ) : null}
    </div>
  );
}
