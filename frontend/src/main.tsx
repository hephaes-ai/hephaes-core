import React from "react";
import ReactDOM from "react-dom/client";
import { RouterProvider } from "react-router-dom";

import { router } from "./app/routes";
import "./styles.css";
import { FeedbackProvider } from "./state/feedback";

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <FeedbackProvider>
      <RouterProvider router={router} />
    </FeedbackProvider>
  </React.StrictMode>,
);
