import { useEffect, useMemo, useRef, useState } from "react";

import assistantAvatar from "@/assets/assistant-avatar.svg";
import { sendAssistantMessage } from "@/shared/api";
import { cx } from "@/shared/lib/cx";
import type { AssistantMode } from "@/shared/types/assistant";

interface ChatMessage {
  id: string;
  role: "assistant" | "user";
  text: string;
  createdAt: number;
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
  "Show top 5 debtors",
  "How many overdue clients do we have?",
  "Show client John Smith",
];

const DEFAULT_SUGGESTIONS_RU = [
  "Сводка по клиентам",
  "Покажи топ-5 должников",
  "Сколько просроченных клиентов?",
  "Покажи клиента John Smith",
];

function resolveSpeechRecognitionConstructor(): SpeechRecognitionConstructorLike | null {
  if (typeof window === "undefined") {
    return null;
  }

  const windowWithSpeech = window as WindowWithSpeechRecognition;
  return windowWithSpeech.SpeechRecognition || windowWithSpeech.webkitSpeechRecognition || null;
}

function generateMessageId(): string {
  return `${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
}

function normalizeOutgoingMessage(rawValue: string): string {
  return rawValue.replace(/\s+/g, " ").trim();
}

function normalizeDisplayMessage(rawValue: string): string {
  return rawValue.trim();
}

function isRussianText(rawValue: string): boolean {
  return /[а-яё]/i.test(rawValue);
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

export function AssistantWidget() {
  const language = useMemo(() => resolveUserLanguage(), []);
  const isRussian = language === "ru";
  const defaultSuggestions = isRussian ? DEFAULT_SUGGESTIONS_RU : DEFAULT_SUGGESTIONS_EN;
  const speechRecognitionSupported = useMemo(() => resolveSpeechRecognitionConstructor() !== null, []);

  const [isOpen, setIsOpen] = useState(false);
  const [mode, setMode] = useState<AssistantMode>("text");
  const [draft, setDraft] = useState("");
  const [messages, setMessages] = useState<ChatMessage[]>([
    {
      id: generateMessageId(),
      role: "assistant",
      text: isRussian
        ? "Привет. Я могу ответить по вашим клиентским данным: сводка, долги, просрочки, статус конкретного клиента."
        : "Hi. I can answer from your internal client data: summaries, debt, overdue status, and specific client details.",
      createdAt: Date.now(),
    },
  ]);
  const [suggestions, setSuggestions] = useState<string[]>(defaultSuggestions);
  const [isSending, setIsSending] = useState(false);
  const [isListening, setIsListening] = useState(false);
  const [isSpeaking, setIsSpeaking] = useState(false);
  const [voiceError, setVoiceError] = useState("");

  const isMountedRef = useRef(true);
  const recognitionRef = useRef<SpeechRecognitionLike | null>(null);
  const abortControllerRef = useRef<AbortController | null>(null);
  const messagesEndRef = useRef<HTMLDivElement | null>(null);

  const hasSendableText = normalizeOutgoingMessage(draft).length > 0;

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
      if (typeof window !== "undefined" && "speechSynthesis" in window) {
        window.speechSynthesis.cancel();
      }
    };
  }, []);

  function appendMessage(role: ChatMessage["role"], text: string): void {
    const normalizedText = normalizeDisplayMessage(text);
    if (!normalizedText) {
      return;
    }

    setMessages((previous) => [
      ...previous,
      {
        id: generateMessageId(),
        role,
        text: normalizedText,
        createdAt: Date.now(),
      },
    ]);
  }

  function speakText(rawText: string): void {
    if (typeof window === "undefined" || !("speechSynthesis" in window)) {
      return;
    }

    const text = normalizeDisplayMessage(rawText);
    if (!text) {
      return;
    }

    window.speechSynthesis.cancel();
    const utterance = new SpeechSynthesisUtterance(text);
    utterance.lang = isRussianText(text) ? "ru-RU" : "en-US";
    utterance.rate = 1;
    utterance.pitch = 1;
    utterance.onstart = () => {
      if (isMountedRef.current) {
        setIsSpeaking(true);
      }
    };
    utterance.onend = () => {
      if (isMountedRef.current) {
        setIsSpeaking(false);
      }
    };
    utterance.onerror = () => {
      if (isMountedRef.current) {
        setIsSpeaking(false);
      }
    };

    window.speechSynthesis.speak(utterance);
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
      const response = await sendAssistantMessage(outgoingText, mode, controller.signal);
      const replyText = normalizeDisplayMessage(response.reply || "");

      if (replyText) {
        appendMessage("assistant", replyText);
        if (mode === "voice") {
          speakText(replyText);
        }
      }

      if (Array.isArray(response.suggestions) && response.suggestions.length) {
        setSuggestions(response.suggestions.slice(0, 8));
      }
    } catch (error) {
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
    const lastAssistantMessage = [...messages].reverse().find((message) => message.role === "assistant");
    if (!lastAssistantMessage) {
      return;
    }

    speakText(lastAssistantMessage.text);
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
                <strong>{isRussian ? "CBooster Помощник" : "CBooster Assistant"}</strong>
                <span>{isRussian ? "Внутренние данные клиентов" : "Internal client data"}</span>
              </div>
            </div>
            <button
              type="button"
              className="assistant-widget__close-btn"
              aria-label={isRussian ? "Закрыть" : "Close"}
              onClick={() => setIsOpen(false)}
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
                <p>{message.text}</p>
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
                  {isRussian ? "Озвучить ответ" : "Speak Reply"}
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
