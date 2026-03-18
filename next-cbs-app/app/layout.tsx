import "./globals.css";
import type { ReactNode } from "react";

export const metadata = {
  title: "Lightweight CBS Push",
  description: "Simple UI to test CBS push independent of Tomcat/OpenMRS"
};

export default function RootLayout({ children }: { children: ReactNode }) {
  return (
    <html lang="en">
      <body className="app-root">{children}</body>
    </html>
  );
}

