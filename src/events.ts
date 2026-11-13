import { filter } from "rxjs";
import type { Observable } from "rxjs";
import { RxEventType } from "./constants.js";
import type { RxEventType as RxEventTypeName } from "./constants.js";
import type {
  RxCloseEvent,
  RxDisconnectEvent,
  RxExitEvent,
  RxMessageEvent,
  RxOutputEvent,
  RxProcessEvent,
  RxSpawnEvent,
  RxStderrEvent,
  RxStdoutEvent,
} from "./types.js";

const EVENT_TYPES = new Set<string>(Object.values(RxEventType));

/** 从事件联合中提取指定名称的成员。 */
export type RxEventOfType<
  TMessage,
  TOutput extends Buffer | string,
  TType extends RxEventTypeName,
> = Extract<RxProcessEvent<TMessage, TOutput>, { readonly type: TType }>;

/** 判断任意值是否为合法的进程事件名称。 */
export function isRxEventType(value: unknown): value is RxEventTypeName {
  return typeof value === "string" && EVENT_TYPES.has(value);
}

/** 判断事件是否为 spawn 事件。 */
export function isSpawnEvent<TMessage, TOutput extends Buffer | string>(
  event: RxProcessEvent<TMessage, TOutput>,
): event is RxSpawnEvent {
  return event.type === RxEventType.Spawn;
}

/** 判断事件是否为 stdout 数据事件。 */
export function isStdoutEvent<TMessage, TOutput extends Buffer | string>(
  event: RxProcessEvent<TMessage, TOutput>,
): event is RxStdoutEvent<TOutput> {
  return event.type === RxEventType.Stdout;
}

/** 判断事件是否为 stderr 数据事件。 */
export function isStderrEvent<TMessage, TOutput extends Buffer | string>(
  event: RxProcessEvent<TMessage, TOutput>,
): event is RxStderrEvent<TOutput> {
  return event.type === RxEventType.Stderr;
}

/** 判断事件是否来自 stdout 或 stderr。 */
export function isOutputEvent<TMessage, TOutput extends Buffer | string>(
  event: RxProcessEvent<TMessage, TOutput>,
): event is RxOutputEvent<TOutput> {
  return (
    event.type === RxEventType.Stdout ||
    event.type === RxEventType.Stderr
  );
}

/** 判断事件是否为 fork IPC 消息事件。 */
export function isMessageEvent<TMessage, TOutput extends Buffer | string>(
  event: RxProcessEvent<TMessage, TOutput>,
): event is RxMessageEvent<TMessage> {
  return event.type === RxEventType.Message;
}

/** 判断事件是否为 IPC 通道断开事件。 */
export function isDisconnectEvent<TMessage, TOutput extends Buffer | string>(
  event: RxProcessEvent<TMessage, TOutput>,
): event is RxDisconnectEvent {
  return event.type === RxEventType.Disconnect;
}

/** 判断事件是否为进程退出事件。 */
export function isExitEvent<TMessage, TOutput extends Buffer | string>(
  event: RxProcessEvent<TMessage, TOutput>,
): event is RxExitEvent {
  return event.type === RxEventType.Exit;
}

/** 判断事件是否为进程关闭事件。 */
export function isCloseEvent<TMessage, TOutput extends Buffer | string>(
  event: RxProcessEvent<TMessage, TOutput>,
): event is RxCloseEvent {
  return event.type === RxEventType.Close;
}

/**
 * 筛选指定类型的进程事件，并同步收窄事件类型。
 *
 * 运算符作用于单个上游订阅；若需要分别消费多个通道，应共享同一个源。
 */
export function filterEvent<const TType extends RxEventTypeName>(
  type: TType,
): <TMessage, TOutput extends Buffer | string>(
  source: Observable<RxProcessEvent<TMessage, TOutput>>,
) => Observable<RxEventOfType<TMessage, TOutput, TType>> {
  return <TMessage, TOutput extends Buffer | string>(
    source: Observable<RxProcessEvent<TMessage, TOutput>>,
  ): Observable<RxEventOfType<TMessage, TOutput, TType>> =>
    source.pipe(
      filter(
        (
          event,
        ): event is RxEventOfType<TMessage, TOutput, TType> =>
          event.type === type,
      ),
    );
}
