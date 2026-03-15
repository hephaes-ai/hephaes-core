import {
  createContext,
  type PropsWithChildren,
  useCallback,
  useContext,
  useMemo,
  useState,
} from "react";

type FeedbackTone = "info" | "error";

type FeedbackMessage = {
  id: string;
  tone: FeedbackTone;
  message: string;
};

type FeedbackContextValue = {
  messages: FeedbackMessage[];
  pushFeedback: (message: Omit<FeedbackMessage, "id">) => void;
  dismissFeedback: (id: string) => void;
};

const FeedbackContext = createContext<FeedbackContextValue | null>(null);

export function FeedbackProvider({ children }: PropsWithChildren) {
  const [messages, setMessages] = useState<FeedbackMessage[]>([]);

  const dismissFeedback = useCallback((id: string) => {
    setMessages((current) => current.filter((message) => message.id !== id));
  }, []);

  const pushFeedback = useCallback((message: Omit<FeedbackMessage, "id">) => {
    const id = `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;

    setMessages((current) => {
      const next = [...current, { ...message, id }];
      return next.slice(-4);
    });
  }, []);

  const value = useMemo(
    () => ({
      messages,
      pushFeedback,
      dismissFeedback,
    }),
    [dismissFeedback, messages, pushFeedback],
  );

  return <FeedbackContext.Provider value={value}>{children}</FeedbackContext.Provider>;
}

export function useFeedback() {
  const value = useContext(FeedbackContext);
  if (value === null) {
    throw new Error("useFeedback must be used inside FeedbackProvider");
  }
  return value;
}
