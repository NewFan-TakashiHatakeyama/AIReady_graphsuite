import { CircleHelp } from 'lucide-react'
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/Tooltip'

interface HoverHelpLabelProps {
  label: string
  helpText: string
}

export default function HoverHelpLabel({ label, helpText }: HoverHelpLabelProps) {
  return (
    <span className="inline-flex items-center gap-1.5">
      <span>{label}</span>
      <TooltipProvider>
        <Tooltip>
          <TooltipTrigger asChild>
            <span
              className="inline-flex h-4 w-4 items-center justify-center rounded-full text-muted-foreground hover:text-foreground"
              aria-label={`${label} の説明`}
            >
              <CircleHelp className="h-3.5 w-3.5" />
            </span>
          </TooltipTrigger>
          <TooltipContent side="top" align="center" className="max-w-[300px]">
            {helpText}
          </TooltipContent>
        </Tooltip>
      </TooltipProvider>
    </span>
  )
}
