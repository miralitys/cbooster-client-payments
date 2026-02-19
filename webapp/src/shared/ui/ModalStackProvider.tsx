import { useCallback, useMemo, useRef } from "react";
import type { ReactNode } from "react";

import { ModalStackContext, type ModalStackContextValue } from "@/shared/ui/modalStackContext";

interface ModalStackProviderProps {
  children: ReactNode;
}

export function ModalStackProvider({ children }: ModalStackProviderProps) {
  const stackRef = useRef<symbol[]>([]);

  const registerModal = useCallback((id: symbol) => {
    const stack = stackRef.current;
    if (!stack.includes(id)) {
      stack.push(id);
    }
  }, []);

  const unregisterModal = useCallback((id: symbol) => {
    const stack = stackRef.current;
    const index = stack.lastIndexOf(id);
    if (index >= 0) {
      stack.splice(index, 1);
    }
  }, []);

  const isTopmostModal = useCallback((id: symbol) => {
    const stack = stackRef.current;
    return stack.length > 0 && stack[stack.length - 1] === id;
  }, []);

  const value = useMemo<ModalStackContextValue>(
    () => ({
      registerModal,
      unregisterModal,
      isTopmostModal,
    }),
    [isTopmostModal, registerModal, unregisterModal],
  );

  return <ModalStackContext.Provider value={value}>{children}</ModalStackContext.Provider>;
}
