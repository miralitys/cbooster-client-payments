import { createContext } from "react";

export interface ModalStackContextValue {
  registerModal: (id: symbol) => void;
  unregisterModal: (id: symbol) => void;
  isTopmostModal: (id: symbol) => boolean;
}

export const ModalStackContext = createContext<ModalStackContextValue | null>(null);
