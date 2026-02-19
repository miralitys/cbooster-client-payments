import { useEffect } from "react";
import type { ReactNode } from "react";

interface ModalProps {
  open: boolean;
  title: string;
  onClose: () => void;
  children: ReactNode;
  footer?: ReactNode;
}

export function Modal({ open, title, onClose, children, footer }: ModalProps) {
  useEffect(() => {
    if (!open) {
      return;
    }

    function onKeyDown(event: KeyboardEvent) {
      if (event.key === "Escape") {
        onClose();
      }
    }

    document.addEventListener("keydown", onKeyDown);
    return () => {
      document.removeEventListener("keydown", onKeyDown);
    };
  }, [open, onClose]);

  if (!open) {
    return null;
  }

  return (
    <div className="cb-modal" role="dialog" aria-modal="true" aria-label={title}>
      <button className="cb-modal__backdrop" onClick={onClose} aria-label="Close modal" />
      <div className="cb-modal__dialog">
        <header className="cb-modal__header">
          <h3>{title}</h3>
          <button className="cb-modal__close" onClick={onClose} aria-label="Close">
            ×
          </button>
        </header>
        <div className="cb-modal__content">{children}</div>
        {footer ? <footer className="cb-modal__footer">{footer}</footer> : null}
      </div>
    </div>
  );
}
