import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/tooltip";

export function DirectionTag({ direction }: { direction: string }) {
  if (direction === "increases_risk")
    return (
      <Tooltip>
        <TooltipTrigger
          render={<span />}
          className="inline-flex items-center px-2 py-0.5 text-xs font-medium bg-direction-up-bg text-direction-up cursor-default"
        >
          Raises denial risk
        </TooltipTrigger>
        <TooltipContent side="top">
          This feature pushed the model toward predicting denial. Higher
          contribution = stronger signal.
        </TooltipContent>
      </Tooltip>
    );
  if (direction === "decreases_risk")
    return (
      <Tooltip>
        <TooltipTrigger
          render={<span />}
          className="inline-flex items-center px-2 py-0.5 text-xs font-medium bg-direction-down-bg text-direction-down cursor-default"
        >
          Lowers denial risk
        </TooltipTrigger>
        <TooltipContent side="top">
          This feature pulled the model away from predicting denial.
        </TooltipContent>
      </Tooltip>
    );
  return (
    <span className="inline-flex items-center px-2 py-0.5 text-xs font-medium bg-muted text-muted-foreground">
      Neutral
    </span>
  );
}
