import { useEffect, useRef } from "react";
import type { ReactNode } from "react";

import { useModalStack } from "@/shared/ui/useModalStack";

interface ModalProps {
  open: boolean;
  title: string;
  onClose: () => void;
  children: ReactNode;
  footer?: ReactNode;
  dialogClassName?: string;
  headerClassName?: string;
  contentClassName?: string;
  footerClassName?: string;
}

export function Modal({
  open,
  title,
  onClose,
  children,
  footer,
  dialogClassName = "",
  headerClassName = "",
  contentClassName = "",
  footerClassName = "",
}: ModalProps) {
  const dialogRef = useRef<HTMLDivElement | null>(null);
  const modalIdRef = useRef<symbol>(Symbol("cb-modal"));
  const returnFocusRef = useRef<HTMLElement | null>(null);
  const { registerModal, unregisterModal, isTopmostModal: isTopmostFromStack } = useModalStack();

  function isTopmostCurrentModal(): boolean {
    return isTopmostFromStack(modalIdRef.current);
  }

  function requestClose() {
    if (!isTopmostCurrentModal()) {
      return;
    }
    onClose();
  }

  useEffect(() => {
    if (!open) {
      return;
    }

    const modalId = modalIdRef.current;
    registerModal(modalId);
    returnFocusRef.current = document.activeElement instanceof HTMLElement ? document.activeElement : null;

    const dialog = dialogRef.current;
    const focusableElements = getFocusableElements(dialog);
    if (focusableElements.length > 0) {
      focusableElements[0].focus();
    } else {
      dialog?.focus();
    }

    function onKeyDown(event: KeyboardEvent) {
      if (!isTopmostFromStack(modalId)) {
        return;
      }

      if (event.key === "Escape") {
        event.preventDefault();
        onClose();
        return;
      }

      if (event.key !== "Tab") {
        return;
      }

      const focusables = getFocusableElements(dialogRef.current);
      if (!focusables.length) {
        event.preventDefault();
        dialogRef.current?.focus();
        return;
      }

      const first = focusables[0];
      const last = focusables[focusables.length - 1];
      const current = document.activeElement;

      if (event.shiftKey) {
        if (current === first || !dialogRef.current?.contains(current)) {
          event.preventDefault();
          last.focus();
        }
        return;
      }

      if (current === last || !dialogRef.current?.contains(current)) {
        event.preventDefault();
        first.focus();
      }
    }

    document.addEventListener("keydown", onKeyDown);
    return () => {
      document.removeEventListener("keydown", onKeyDown);

      unregisterModal(modalId);

      if (returnFocusRef.current && document.contains(returnFocusRef.current)) {
        returnFocusRef.current.focus();
      }

      returnFocusRef.current = null;
    };
  }, [isTopmostFromStack, onClose, open, registerModal, unregisterModal]);

  if (!open) {
    return null;
  }

  return (
    <div className="cb-modal" role="dialog" aria-modal="true" aria-label={title}>
      <button className="cb-modal__backdrop" onClick={requestClose} aria-label="Close modal" />
      <div ref={dialogRef} className={["cb-modal__dialog", dialogClassName].filter(Boolean).join(" ")} tabIndex={-1}>
        <header className={["cb-modal__header", headerClassName].filter(Boolean).join(" ")}>
          <h3>{title}</h3>
          <button className="cb-modal__close" onClick={requestClose} aria-label="Close">
            ×
          </button>
        </header>
        <div className={["cb-modal__content", contentClassName].filter(Boolean).join(" ")}>{children}</div>
        {footer ? <footer className={["cb-modal__footer", footerClassName].filter(Boolean).join(" ")}>{footer}</footer> : null}
      </div>
    </div>
  );
}

function getFocusableElements(root: HTMLElement | null): HTMLElement[] {
  if (!root) {
    return [];
  }

  const selectors = [
    "a[href]",
    "button:not([disabled])",
    "input:not([disabled]):not([type='hidden'])",
    "select:not([disabled])",
    "textarea:not([disabled])",
    "[tabindex]:not([tabindex='-1'])",
  ].join(",");

  return [...root.querySelectorAll<HTMLElement>(selectors)].filter((element) => !element.hasAttribute("disabled"));
}
