"use client";

import { useRef, useState } from "react";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { toast } from "sonner";
import {
  Select,
  SelectContent,
  SelectGroup,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";

export function StatusControl({
  claimId,
  initialStatus,
}: {
  claimId: string;
  initialStatus: string;
}) {
  const queryClient = useQueryClient();
  const [status, setStatus] = useState(initialStatus);
  const previousStatusRef = useRef(initialStatus);

  const mutation = useMutation({
    mutationFn: async (newStatus: string) => {
      const res = await fetch(`/api/claims/${claimId}/status`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ status: newStatus }),
      });
      if (!res.ok) throw new Error("Failed to update status");
      return res.json();
    },
    onMutate: async () => {
      previousStatusRef.current = status;
    },
    onSuccess: (_, newStatus) => {
      setStatus(newStatus);
      const label = newStatus.charAt(0).toUpperCase() + newStatus.slice(1);
      const undoStatus = previousStatusRef.current;
      toast(`Marked as ${label}`, {
        action: {
          label: "Undo",
          onClick: () => mutation.mutate(undoStatus),
        },
        duration: 5000,
      });
      queryClient.invalidateQueries({ queryKey: ["claims"] });
      queryClient.invalidateQueries({ queryKey: ["claim-status", claimId] });
    },
    onError: () => toast.error("Could not update status"),
  });

  const items = [
    { value: "new", label: "New" },
    { value: "reviewed", label: "Reviewed" },
    { value: "actioned", label: "Actioned" },
  ] as const;

  return (
    <Select
      items={items}
      value={status}
      onValueChange={(v) => {
        if (v && v !== status) mutation.mutate(v);
      }}
      disabled={mutation.isPending}
    >
      <SelectTrigger
        className="w-36 h-10 pointer-coarse:h-11 text-xs"
        aria-label="Claim status"
      >
        <SelectValue />
      </SelectTrigger>
      <SelectContent>
        <SelectGroup>
          {items.map((item) => (
            <SelectItem key={item.value} value={item.value}>
              {item.label}
            </SelectItem>
          ))}
        </SelectGroup>
      </SelectContent>
    </Select>
  );
}
