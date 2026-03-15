import { useFeedback } from "../state/feedback";

export function FeedbackViewport() {
  const { messages, dismissFeedback } = useFeedback();

  if (messages.length === 0) {
    return null;
  }

  return (
    <aside className="feedback-stack" aria-live="polite" aria-label="Notifications">
      {messages.map((message) => (
        <article
          key={message.id}
          className={
            message.tone === "error" ? "feedback-card feedback-card-error" : "feedback-card"
          }
        >
          <div>
            <p className="feedback-title">{message.tone === "error" ? "Problem" : "Update"}</p>
            <p className="feedback-message">{message.message}</p>
          </div>
          <button
            type="button"
            className="feedback-dismiss"
            onClick={() => dismissFeedback(message.id)}
          >
            Dismiss
          </button>
        </article>
      ))}
    </aside>
  );
}
