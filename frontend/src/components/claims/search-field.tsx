"use client";

import { useEffect, useRef, useState } from "react";
import { MagnifyingGlass } from "@phosphor-icons/react";
import { Input } from "@/components/ui/input";

export function SearchField({
  currentSearch,
  inputRef,
  onCommit,
}: {
  currentSearch: string;
  inputRef: { current: HTMLInputElement | null };
  onCommit: (value: string) => void;
}) {
  const [search, setSearch] = useState(currentSearch);
  const timerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  useEffect(() => {
    return () => {
      if (timerRef.current) {
        clearTimeout(timerRef.current);
      }
    };
  }, []);

  return (
    <div className="relative">
      <MagnifyingGlass
        aria-hidden="true"
        className="pointer-events-none absolute top-1/2 left-2.5 size-3.5 -translate-y-1/2 text-muted-foreground"
      />
      <Input
        ref={inputRef}
        aria-label="Search by claim ID"
        className="w-full pl-8 pr-7 sm:w-56"
        onChange={(event) => {
          const nextValue = event.target.value;
          setSearch(nextValue);

          if (timerRef.current) {
            clearTimeout(timerRef.current);
          }

          timerRef.current = setTimeout(() => {
            onCommit(nextValue);
          }, 300);
        }}
        placeholder="Search claim ID…"
        value={search}
      />
      <kbd className="pointer-events-none absolute top-1/2 right-2 -translate-y-1/2 font-mono type-caption text-muted-foreground">
        /
      </kbd>
    </div>
  );
}
