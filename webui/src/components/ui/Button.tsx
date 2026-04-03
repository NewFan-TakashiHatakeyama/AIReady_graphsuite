import * as React from 'react'
import { Slot } from '@radix-ui/react-slot'
import { cva, type VariantProps } from 'class-variance-authority'
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/Tooltip'
import { cn } from '@/lib/utils'

// eslint-disable-next-line react-refresh/only-export-components
export const buttonVariants = cva(
  'inline-flex items-center justify-center gap-2 whitespace-nowrap rounded-md text-sm font-medium ring-offset-background transition-all focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 disabled:pointer-events-none disabled:opacity-50 liquid-glass-interactive liquid-glass-control [&_svg]:pointer-events-none [&_svg]:size-4 [&_svg]:shrink-0',
  {
    variants: {
      variant: {
        default:
          'liquid-glass-surface liquid-glass-button-default text-[hsl(224_78%_12%)] dark:text-primary-foreground',
        destructive: 'bg-destructive text-destructive-foreground',
        outline:
          'liquid-glass-surface bg-background/55 text-[hsl(224_78%_12%)] dark:text-accent-foreground',
        secondary:
          'liquid-glass-surface bg-secondary/70 text-[hsl(224_78%_12%)] dark:text-secondary-foreground',
        ghost:
          'bg-transparent text-[hsl(224_78%_12%)] dark:text-foreground',
        link: 'text-primary underline-offset-4'
      },
      size: {
        default: 'h-10 px-4 py-2',
        sm: 'h-9 rounded-md px-3',
        lg: 'h-11 rounded-md px-8',
        icon: 'size-8'
      }
    },
    defaultVariants: {
      variant: 'default',
      size: 'default'
    }
  }
)

interface ButtonProps
  extends React.ButtonHTMLAttributes<HTMLButtonElement>,
    VariantProps<typeof buttonVariants> {
  asChild?: boolean
  side?: 'top' | 'right' | 'bottom' | 'left'
  tooltip?: string
}

const Button = React.forwardRef<HTMLButtonElement, ButtonProps>(
  ({ className, variant, tooltip, size, side = 'right', asChild = false, ...props }, ref) => {
    const Comp = asChild ? Slot : 'button'
    if (!tooltip) {
      return (
        <Comp
          className={cn(buttonVariants({ variant, size, className }), 'cursor-pointer')}
          ref={ref}
          {...props}
        />
      )
    }

    return (
      <TooltipProvider>
        <Tooltip>
          <TooltipTrigger asChild>
            <Comp
              className={cn(buttonVariants({ variant, size, className }), 'cursor-pointer')}
              ref={ref}
              {...props}
            />
          </TooltipTrigger>
          <TooltipContent side={side}>{tooltip}</TooltipContent>
        </Tooltip>
      </TooltipProvider>
    )
  }
)
Button.displayName = 'Button'

export type ButtonVariantType = Exclude<
  NonNullable<Parameters<typeof buttonVariants>[0]>['variant'],
  undefined
>

export default Button
