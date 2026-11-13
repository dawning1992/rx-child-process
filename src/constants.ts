/** 资源限制的语义化取值。 */
export const RxLimits = {
  /** 显式关闭超时、字节数或消息数量限制。 */
  Disabled: Number.POSITIVE_INFINITY,
} as const;

/** 子进程事件名称常量。 */
export const RxEventType = {
  Spawn: "spawn",
  Stdout: "stdout",
  Stderr: "stderr",
  Message: "message",
  Disconnect: "disconnect",
  Exit: "exit",
  Close: "close",
} as const;

/** 所有子进程事件名称。 */
export type RxEventType = (typeof RxEventType)[keyof typeof RxEventType];
