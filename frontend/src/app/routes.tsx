import { Navigate, createBrowserRouter } from "react-router-dom";

import { AppShell } from "./AppShell";
import { AssetDetailPage } from "../pages/AssetDetailPage";
import { InventoryPage } from "../pages/InventoryPage";
import { NotFoundPage } from "../pages/NotFoundPage";

export const router = createBrowserRouter([
  {
    path: "/",
    element: <AppShell />,
    children: [
      {
        index: true,
        element: <InventoryPage />,
      },
      {
        path: "assets/:assetId",
        element: <AssetDetailPage />,
      },
    ],
  },
  {
    path: "/inventory",
    element: <Navigate to="/" replace />,
  },
  {
    path: "*",
    element: <NotFoundPage />,
  },
]);
