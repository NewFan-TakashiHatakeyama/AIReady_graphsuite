import { ReactNode, useEffect, useRef, useState } from 'react'
import { cn } from '@/lib/utils'

type ChartSize = {
  width: number
  height: number
}

type ChartContainerProps = {
  className?: string
  minHeight?: number
  children: ReactNode | ((size: ChartSize) => ReactNode)
}

export default function ChartContainer({
  className,
  minHeight = 72,
  children
}: ChartContainerProps) {
  const hostRef = useRef<HTMLDivElement | null>(null)
  const [isReady, setIsReady] = useState(false)
  const [size, setSize] = useState<ChartSize>({ width: 0, height: 0 })

  useEffect(() => {
    const host = hostRef.current
    if (!host) return

    const updateReady = () => {
      const width = host.clientWidth
      const height = host.clientHeight
      setSize({ width, height })
      setIsReady(width > 0 && height > 0)
    }

    updateReady()
    const observer = new ResizeObserver(() => updateReady())
    observer.observe(host)
    return () => observer.disconnect()
  }, [])

  return (
    <div ref={hostRef} className={cn('relative h-full w-full', className)} style={{ minHeight }}>
      {isReady
        ? (typeof children === 'function' ? children(size) : children)
        : <div className="h-full w-full" aria-hidden="true" />}
    </div>
  )
}
