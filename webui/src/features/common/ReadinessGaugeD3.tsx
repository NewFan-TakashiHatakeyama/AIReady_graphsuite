import { useEffect, useRef, useState } from 'react'
import { scaleLinear } from 'd3'

interface ReadinessGaugeD3Props {
  value: number
  target?: number
}

const clamp = (v: number): number => Math.max(0, Math.min(100, v))
const easeOutCubic = (t: number): number => 1 - Math.pow(1 - t, 3)
const scoreColorHex = (score: number): string => {
  if (score < 50) return '#dc2626'
  if (score < 70) return '#f59e0b'
  if (score < 90) return '#16a34a'
  return '#2563eb'
}

const polarToCartesian = (cx: number, cy: number, r: number, angleRad: number) => ({
  x: cx + r * Math.cos(angleRad),
  y: cy + r * Math.sin(angleRad)
})

const describeArcStroke = (
  cx: number,
  cy: number,
  r: number,
  startAngle: number,
  endAngle: number
): string => {
  const segments = 72
  const points = Array.from({ length: segments + 1 }, (_, i) => {
    const t = i / segments
    const angle = startAngle + (endAngle - startAngle) * t
    return polarToCartesian(cx, cy, r, angle)
  })
  return points.map((p, idx) => `${idx === 0 ? 'M' : 'L'} ${p.x} ${p.y}`).join(' ')
}

export default function ReadinessGaugeD3({ value, target = 90 }: ReadinessGaugeD3Props) {
  const normalizedValue = clamp(value)
  const normalizedTarget = clamp(target)
  const [animatedValue, setAnimatedValue] = useState(0)
  const animatedValueRef = useRef(0)

  useEffect(() => {
    const from = animatedValueRef.current
    const to = normalizedValue
    const durationMs = 900
    const start = performance.now()
    let frameId = 0

    const tick = (now: number) => {
      const progress = Math.min(1, (now - start) / durationMs)
      const eased = easeOutCubic(progress)
      const next = from + (to - from) * eased
      animatedValueRef.current = next
      setAnimatedValue(next)
      if (progress < 1) {
        frameId = window.requestAnimationFrame(tick)
      }
    }

    frameId = window.requestAnimationFrame(tick)
    return () => window.cancelAnimationFrame(frameId)
  }, [normalizedValue])

  const width = 520
  const height = 250
  const cx = width / 2
  const cy = 205
  const outerRadius = 150
  const ringWidth = 56

  // Top semicircle: 0% = left, 100% = right
  const toAngle = scaleLinear().domain([0, 100]).range([-Math.PI, 0])
  const valueAngle = toAngle(animatedValue)
  const targetAngle = toAngle(normalizedTarget)

  const baseArc = describeArcStroke(cx, cy, outerRadius, -Math.PI, 0)
  const valueArc = describeArcStroke(cx, cy, outerRadius, -Math.PI, valueAngle)

  const tipR = outerRadius - 4
  const baseR = 14
  const baseW = 10
  const tipX = cx + Math.cos(valueAngle) * tipR
  const tipY = cy + Math.sin(valueAngle) * tipR
  const baseCx = cx + Math.cos(valueAngle + Math.PI) * baseR
  const baseCy = cy + Math.sin(valueAngle + Math.PI) * baseR
  const leftX = baseCx + Math.cos(valueAngle + Math.PI / 2) * baseW
  const leftY = baseCy + Math.sin(valueAngle + Math.PI / 2) * baseW
  const rightX = baseCx + Math.cos(valueAngle - Math.PI / 2) * baseW
  const rightY = baseCy + Math.sin(valueAngle - Math.PI / 2) * baseW
  const needlePoints = `${tipX},${tipY} ${leftX},${leftY} ${rightX},${rightY}`

  const markerOuter = outerRadius + 8
  const markerInner = outerRadius - ringWidth - 10
  const markerX1 = cx + Math.cos(targetAngle) * markerInner
  const markerY1 = cy + Math.sin(targetAngle) * markerInner
  const markerX2 = cx + Math.cos(targetAngle) * markerOuter
  const markerY2 = cy + Math.sin(targetAngle) * markerOuter

  const valueColor = scoreColorHex(animatedValue)

  const zones = [
    { from: 0, to: 50, color: '#dc2626' },
    { from: 50, to: 70, color: '#f59e0b' },
    { from: 70, to: 90, color: '#16a34a' },
    { from: 90, to: 100, color: '#2563eb' }
  ]
  const ticks = Array.from({ length: 11 }, (_, i) => i * 10)

  return (
    <svg viewBox={`0 0 ${width} ${height}`} className="h-full w-full">
      {zones.map((zone) => (
        <path
          key={`${zone.from}-${zone.to}`}
          d={describeArcStroke(cx, cy, outerRadius, toAngle(zone.from), toAngle(zone.to))}
          stroke={zone.color}
          strokeWidth={ringWidth}
          strokeLinecap="butt"
          fill="none"
          opacity={0.22}
        />
      ))}
      <path d={baseArc} stroke="#e5e7eb" strokeWidth={ringWidth} strokeLinecap="butt" fill="none" />
      <path d={valueArc} stroke={valueColor} strokeWidth={ringWidth} strokeLinecap="butt" fill="none" />

      {ticks.map((tick) => {
        const a = toAngle(tick)
        const p1 = polarToCartesian(cx, cy, outerRadius + 2, a)
        const p2 = polarToCartesian(cx, cy, outerRadius + (tick % 20 === 0 ? 12 : 8), a)
        const labelPoint = polarToCartesian(cx, cy, outerRadius + 44, a)
        return (
          <g key={`tick-${tick}`}>
            <line
              x1={p1.x}
              y1={p1.y}
              x2={p2.x}
              y2={p2.y}
              stroke="#6b7280"
              strokeWidth={tick % 20 === 0 ? 1.5 : 1}
              opacity={0.75}
            />
            {tick !== 0 && tick !== 100 && (
              <text
                x={labelPoint.x}
                y={labelPoint.y + 4}
                textAnchor="middle"
                className="fill-muted-foreground text-[9px]"
              >
                {tick}
              </text>
            )}
          </g>
        )
      })}

      <line x1={markerX1} y1={markerY1} x2={markerX2} y2={markerY2} stroke="#111827" strokeWidth={3} />

      <polygon points={needlePoints} fill="#111827" />
      <circle cx={cx} cy={cy} r={10} fill="#111827" />
      <circle cx={cx} cy={cy} r={7} fill="#ffffff" />

      <text x={36} y={244} className="fill-muted-foreground text-[18px] font-semibold">
        0%
      </text>
      <text x={width - 72} y={244} className="fill-muted-foreground text-[18px] font-semibold">
        100%
      </text>
    </svg>
  )
}
