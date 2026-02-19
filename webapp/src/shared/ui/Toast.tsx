interface ToastProps {
  kind?: "info" | "success" | "error";
  message: string;
  onClose?: () => void;
}

export function Toast({ kind = "info", message, onClose }: ToastProps) {
  return (
    <div className={`cb-toast cb-toast--${kind}`.trim()} role="status" aria-live="polite">
      <span>{message}</span>
      {onClose ? (
        <button type="button" className="cb-toast__close" onClick={onClose}>
          ×
        </button>
      ) : null}
    </div>
  );
}
