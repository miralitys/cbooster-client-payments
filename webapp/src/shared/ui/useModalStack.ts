import { useContext } from "react";

import { ModalStackContext, type ModalStackContextValue } from "@/shared/ui/modalStackContext";

export function useModalStack(): ModalStackContextValue {
  const context = useContext(ModalStackContext);
  if (!context) {
    throw new Error("useModalStack must be used inside ModalStackProvider.");
  }
  return context;
}
