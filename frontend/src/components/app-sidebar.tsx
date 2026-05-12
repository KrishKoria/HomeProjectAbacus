"use client";

import Link from "next/link";
import { usePathname, useRouter } from "next/navigation";
import { useEffect } from "react";
import {
  Sidebar,
  SidebarContent,
  SidebarGroup,
  SidebarGroupContent,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
  SidebarHeader,
  SidebarFooter,
} from "@/components/ui/sidebar";
import { useQuery } from "@tanstack/react-query";
import { useTheme } from "next-themes";
import {
  ChartBarIcon,
  SunDimIcon,
  MoonIcon,
  ClipboardTextIcon,
  SignOutIcon,
} from "@phosphor-icons/react";

const navItems = [
  {
    label: "Claims",
    href: "/claims",
    icon: ClipboardTextIcon,
    shortcut: "G + C",
  },
  {
    label: "Dashboard",
    href: "/dashboard",
    icon: ChartBarIcon,
    shortcut: "G + D",
  },
];

export function AppSidebar() {
  const pathname = usePathname();
  const router = useRouter();

  useEffect(() => {
    let gPressed = false;
    let timer: ReturnType<typeof setTimeout>;

    function onKey(e: KeyboardEvent) {
      const tag = (document.activeElement as HTMLElement)?.tagName;
      if (tag === "INPUT" || tag === "TEXTAREA") return;
      if (e.metaKey || e.ctrlKey || e.altKey) return;

      if (e.key === "g") {
        gPressed = true;
        clearTimeout(timer);
        timer = setTimeout(() => {
          gPressed = false;
        }, 500);
        return;
      }

      if (gPressed) {
        if (e.key === "c") {
          e.preventDefault();
          router.push("/claims");
        }
        if (e.key === "d") {
          e.preventDefault();
          router.push("/dashboard");
        }
        gPressed = false;
        clearTimeout(timer);
      }
    }

    window.addEventListener("keydown", onKey);
    return () => {
      window.removeEventListener("keydown", onKey);
      clearTimeout(timer);
    };
  }, [router]);

  const { theme, setTheme } = useTheme();

  const sessionQuery = useQuery({
    queryKey: ["session"],
    queryFn: async () => {
      const res = await fetch("/api/me");
      if (!res.ok) return null;
      return res.json() as Promise<{ user: { name: string; email: string } }>;
    },
    staleTime: 60_000,
  });

  const user = sessionQuery.data?.user;

  return (
    <Sidebar>
      <SidebarHeader className="px-4 py-3 border-b border-sidebar-border">
        <Link
          href="/claims"
          className="text-sm font-semibold tracking-tight text-sidebar-foreground hover:text-sidebar-primary transition-colors"
        >
          ClaimOps
        </Link>
      </SidebarHeader>

      <SidebarContent className="pt-2">
        <SidebarGroup>
          <SidebarGroupContent>
            <SidebarMenu>
              {navItems.map((item) => {
                const Icon = item.icon;
                const isActive =
                  item.href === "/claims"
                    ? pathname === "/claims" || pathname.startsWith("/claims/")
                    : pathname === item.href;

                return (
                  <SidebarMenuItem key={item.label}>
                    <SidebarMenuButton
                      isActive={isActive}
                      render={<Link href={item.href} />}
                    >
                      <Icon weight={isActive ? "fill" : "regular"} />
                      <span className="text-sm font-medium">{item.label}</span>
                      {item.shortcut && (
                        <span className="ml-auto text-xs text-muted-foreground font-mono opacity-60">
                          {item.shortcut}
                        </span>
                      )}
                    </SidebarMenuButton>
                  </SidebarMenuItem>
                );
              })}
            </SidebarMenu>
          </SidebarGroupContent>
        </SidebarGroup>
      </SidebarContent>

      <SidebarFooter className="p-4 border-t border-sidebar-border space-y-3">
        <div className="px-1 flex items-center gap-3 flex-wrap">
          <span className="type-caption text-muted-foreground">
            <kbd className="font-mono text-[10px] px-1 border border-sidebar-border">
              /
            </kbd>{" "}
            search
          </span>
          <span className="type-caption text-muted-foreground">
            <kbd className="font-mono text-[10px] px-1 border border-sidebar-border">
              C
            </kbd>{" "}
            chat
          </span>
          <span className="type-caption text-muted-foreground">
            <kbd className="font-mono text-[10px] px-1 border border-sidebar-border">
              G
            </kbd>{" "}
            nav
          </span>
        </div>
        <div className="px-1 flex items-center justify-between">
          <span className="type-caption text-muted-foreground">
            <kbd className="font-mono text-[10px] px-1 border border-sidebar-border">
              ⌘ + B
            </kbd>{" "}
            sidebar
          </span>
          <button
            type="button"
            onClick={() => setTheme(theme === "dark" ? "light" : "dark")}
            className="flex size-6 items-center justify-center text-muted-foreground hover:text-foreground transition-colors"
            aria-label="Toggle theme"
          >
            {theme === "dark" ? (
              <SunDimIcon size={14} />
            ) : (
              <MoonIcon size={14} />
            )}
          </button>
        </div>
        {user && (
          <div className="px-1 space-y-0.5">
            <p className="text-xs font-medium text-sidebar-foreground truncate">
              {user.name}
            </p>
            <p className="type-caption text-muted-foreground truncate">
              {user.email}
            </p>
          </div>
        )}
        <SidebarMenuButton
          render={<button />}
          onClick={() => {
            fetch("/api/auth/sign-out", { method: "POST" }).then(() => {
              window.location.href = "/sign-in";
            });
          }}
          className="w-full text-muted-foreground hover:text-foreground"
        >
          <SignOutIcon />
          <span className="text-sm">Sign out</span>
        </SidebarMenuButton>
      </SidebarFooter>
    </Sidebar>
  );
}
