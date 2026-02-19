import { useEffect, useRef, useState } from "react";

import type { ToastMessage } from "@/shared/lib/toast";
import { acquireToastHost, releaseToastHost, subscribeToasts } from "@/shared/lib/toast";

export function ToastHost() {
  const hostIdRef = useRef(Symbol("cb-toast-host"));
  const [isActiveHost, setIsActiveHost] = useState(false);
  const [toasts, setToasts] = useState<ToastMessage[]>([]);
  const toastTimersRef = useRef(new Map<string, number>());

  useEffect(() => {
    const hostId = hostIdRef.current;
    const isAcquired = acquireToastHost(hostId);
    setIsActiveHost(isAcquired);

    return () => {
      releaseToastHost(hostId);
    };
  }, []);

  useEffect(() => {
    if (!isActiveHost) {
      return;
    }

    const timers = toastTimersRef.current;
    return () => {
      for (const timeoutId of timers.values()) {
        window.clearTimeout(timeoutId);
      }
      timers.clear();
    };
  }, [isActiveHost]);

  useEffect(() => {
    if (!isActiveHost) {
      return;
    }

    const unsubscribe = subscribeToasts((toast) => {
      setToasts((prev) => {
        if (prev.some((item) => item.id === toast.id)) {
          return prev;
        }
        return [...prev, toast];
      });

      const durationMs = Number(toast.durationMs) || 0;
      if (durationMs <= 0) {
        return;
      }

      const timeoutId = window.setTimeout(() => {
        setToasts((prev) => prev.filter((item) => item.id !== toast.id));
        toastTimersRef.current.delete(toast.id);
      }, durationMs);

      toastTimersRef.current.set(toast.id, timeoutId);
    });

    return unsubscribe;
  }, [isActiveHost]);

  function dismissToast(id: string) {
    const timeoutId = toastTimersRef.current.get(id);
    if (timeoutId) {
      window.clearTimeout(timeoutId);
      toastTimersRef.current.delete(id);
    }
    setToasts((prev) => prev.filter((item) => item.id !== id));
  }

  if (!isActiveHost || !toasts.length) {
    return null;
  }

  return (
    <div className="cb-toast-host" role="region" aria-label="Notifications">
      {toasts.map((toast) => (
        <div
          key={toast.id}
          className={`cb-toast cb-toast--${toast.type}`.trim()}
          role={toast.type === "error" ? "alert" : "status"}
          aria-live={toast.type === "error" ? "assertive" : "polite"}
        >
          <div className="cb-toast__content">
            <span>{toast.message}</span>
            {toast.action ? (
              <button type="button" className="cb-toast__action" onClick={toast.action.onClick}>
                {toast.action.label}
              </button>
            ) : null}
          </div>
          <button type="button" className="cb-toast__close" onClick={() => dismissToast(toast.id)} aria-label="Dismiss notification">
            ×
          </button>
        </div>
      ))}
    </div>
  );
}
