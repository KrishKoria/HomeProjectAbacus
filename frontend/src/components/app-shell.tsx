"use client";

import { SidebarTrigger } from "@/components/ui/sidebar";
import { AppSidebar } from "@/components/app-sidebar";
import { SidebarInset } from "@/components/ui/sidebar";
export function AppShell({ children }: { children: React.ReactNode }) {
  return (
    <>
      <AppSidebar />
      <SidebarInset>
        <header className="flex h-8 items-center gap-2 px-4 border-b shrink-0">
          <SidebarTrigger className="size-5" />
        </header>
        <main id="main-content" className="flex-1 p-6 @container/main">
          <div className="mx-auto w-full max-w-6xl space-y-8">
            {children}
          </div>
        </main>
      </SidebarInset>
    </>
  );
}
