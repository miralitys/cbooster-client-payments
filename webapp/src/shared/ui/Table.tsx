import { useEffect, useMemo, useRef, useState, type ReactNode } from "react";

import { cx } from "@/shared/lib/cx";

export type TableDensity = "compact";
export type TableAlign = "left" | "center" | "right";

export interface TableColumn<RowType> {
  key: string;
  label: ReactNode;
  align?: TableAlign;
  className?: string;
  cell: (row: RowType, index: number) => ReactNode;
  headerClassName?: string;
}

interface TableProps<RowType> {
  columns: TableColumn<RowType>[];
  rows: RowType[];
  rowKey: (row: RowType, index: number) => string;
  density?: TableDensity;
  emptyState?: ReactNode;
  className?: string;
  tableClassName?: string;
  onRowClick?: (row: RowType, index: number) => void;
  onRowActivate?: (row: RowType, index: number) => void;
  rowClassName?: (row: RowType, index: number) => string | undefined;
  footer?: ReactNode;
  virtualizeRows?: boolean;
  virtualRowHeight?: number;
  virtualOverscan?: number;
  virtualThreshold?: number;
}

export function Table<RowType>({
  columns,
  rows,
  rowKey,
  density = "compact",
  emptyState,
  className = "",
  tableClassName = "",
  onRowClick,
  onRowActivate,
  rowClassName,
  footer,
  virtualizeRows = false,
  virtualRowHeight = 44,
  virtualOverscan = 6,
  virtualThreshold = 120,
}: TableProps<RowType>) {
  const activationHandler = onRowActivate || onRowClick;
  const wrapperRef = useRef<HTMLDivElement | null>(null);
  const [scrollTop, setScrollTop] = useState(0);
  const [viewportHeight, setViewportHeight] = useState(0);
  const resolvedRowHeight = Math.max(24, Math.floor(virtualRowHeight));
  const resolvedOverscan = Math.max(1, Math.floor(virtualOverscan));
  const shouldVirtualize = virtualizeRows && rows.length >= virtualThreshold;

  useEffect(() => {
    if (!virtualizeRows) {
      return;
    }

    const wrapperElement = wrapperRef.current;
    if (!wrapperElement) {
      return;
    }

    const updateViewportMetrics = () => {
      setViewportHeight(wrapperElement.clientHeight || 0);
      setScrollTop(wrapperElement.scrollTop || 0);
    };
    const handleScroll = () => {
      setScrollTop(wrapperElement.scrollTop || 0);
    };

    updateViewportMetrics();
    wrapperElement.addEventListener("scroll", handleScroll, { passive: true });

    let resizeObserver: ResizeObserver | null = null;
    if (typeof ResizeObserver !== "undefined") {
      resizeObserver = new ResizeObserver(updateViewportMetrics);
      resizeObserver.observe(wrapperElement);
    } else {
      window.addEventListener("resize", updateViewportMetrics);
    }

    return () => {
      wrapperElement.removeEventListener("scroll", handleScroll);
      if (resizeObserver) {
        resizeObserver.disconnect();
      } else {
        window.removeEventListener("resize", updateViewportMetrics);
      }
    };
  }, [virtualizeRows, rows.length]);

  const virtualizationWindow = useMemo(() => {
    if (!shouldVirtualize) {
      return {
        startIndex: 0,
        endIndex: rows.length,
        topSpacerHeight: 0,
        bottomSpacerHeight: 0,
      };
    }

    const viewportHeightForWindow = viewportHeight > 0 ? viewportHeight : resolvedRowHeight * 12;
    const visibleRowsCount = Math.max(1, Math.ceil(viewportHeightForWindow / resolvedRowHeight));
    const startIndex = Math.max(0, Math.floor(scrollTop / resolvedRowHeight) - resolvedOverscan);
    const endIndex = Math.min(rows.length, startIndex + visibleRowsCount + resolvedOverscan * 2);

    return {
      startIndex,
      endIndex,
      topSpacerHeight: startIndex * resolvedRowHeight,
      bottomSpacerHeight: Math.max(0, (rows.length - endIndex) * resolvedRowHeight),
    };
  }, [resolvedOverscan, resolvedRowHeight, rows.length, scrollTop, shouldVirtualize, viewportHeight]);

  useEffect(() => {
    if (!shouldVirtualize) {
      return;
    }

    const wrapperElement = wrapperRef.current;
    if (!wrapperElement) {
      return;
    }

    const maxScrollTop = Math.max(0, rows.length * resolvedRowHeight - viewportHeight);
    if (wrapperElement.scrollTop > maxScrollTop) {
      wrapperElement.scrollTop = maxScrollTop;
    }
  }, [resolvedRowHeight, rows.length, shouldVirtualize, viewportHeight]);

  const visibleRows = shouldVirtualize
    ? rows.slice(virtualizationWindow.startIndex, virtualizationWindow.endIndex)
    : rows;

  return (
    <div ref={wrapperRef} className={cx("cb-table-wrap", className)}>
      <table className={cx("cb-table", `cb-table--${density}`, tableClassName)}>
        <thead>
          <tr>
            {columns.map((column) => (
              <th
                key={column.key}
                className={cx("cb-table__head-cell", alignmentClass(column.align), column.headerClassName)}
              >
                {column.label}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {!rows.length ? (
            <tr>
              <td colSpan={columns.length} className="cb-table__empty">
                {emptyState || "No records"}
              </td>
            </tr>
          ) : (
            <>
              {shouldVirtualize && virtualizationWindow.topSpacerHeight > 0 ? (
                <tr aria-hidden="true" className="cb-table__spacer-row">
                  <td
                    colSpan={columns.length}
                    className="cb-table__spacer-cell"
                    style={{ height: `${virtualizationWindow.topSpacerHeight}px` }}
                  />
                </tr>
              ) : null}
              {visibleRows.map((row, relativeIndex) => {
                const index = shouldVirtualize ? virtualizationWindow.startIndex + relativeIndex : relativeIndex;

                return (
                  <tr
                    key={rowKey(row, index)}
                    className={cx(
                      "cb-table__row",
                      activationHandler && "cb-table__row--activatable",
                      rowClassName?.(row, index),
                    )}
                    onClick={
                      activationHandler
                        ? (event) => {
                            if (shouldIgnoreRowActivation(event.target, event.currentTarget)) {
                              return;
                            }
                            activationHandler(row, index);
                          }
                        : undefined
                    }
                    tabIndex={activationHandler ? 0 : undefined}
                    onKeyDown={
                      activationHandler
                        ? (event) => {
                            if (shouldIgnoreRowActivation(event.target, event.currentTarget)) {
                              return;
                            }
                            if (event.key === "Enter" || event.key === " ") {
                              event.preventDefault();
                              activationHandler(row, index);
                            }
                          }
                        : undefined
                    }
                  >
                    {columns.map((column) => (
                      <td
                        key={`${column.key}-${index}`}
                        className={cx("cb-table__cell", alignmentClass(column.align), column.className)}
                      >
                        {column.cell(row, index)}
                      </td>
                    ))}
                  </tr>
                );
              })}
              {shouldVirtualize && virtualizationWindow.bottomSpacerHeight > 0 ? (
                <tr aria-hidden="true" className="cb-table__spacer-row">
                  <td
                    colSpan={columns.length}
                    className="cb-table__spacer-cell"
                    style={{ height: `${virtualizationWindow.bottomSpacerHeight}px` }}
                  />
                </tr>
              ) : null}
            </>
          )}
        </tbody>
        {footer ? <tfoot>{footer}</tfoot> : null}
      </table>
    </div>
  );
}

function alignmentClass(align: TableAlign | undefined): string {
  switch (align) {
    case "right":
      return "cb-table__cell--align-right";
    case "center":
      return "cb-table__cell--align-center";
    default:
      return "cb-table__cell--align-left";
  }
}

function shouldIgnoreRowActivation(target: EventTarget | null, currentTarget: EventTarget | null): boolean {
  if (!(target instanceof Element) || !(currentTarget instanceof HTMLElement)) {
    return false;
  }

  const interactiveRoot = target.closest(
    [
      "button",
      "a",
      "input",
      "select",
      "textarea",
      "[role='button']",
      "[role='link']",
      "[contenteditable='true']",
      "[data-row-activate-ignore='true']",
    ].join(","),
  );

  return Boolean(interactiveRoot && currentTarget.contains(interactiveRoot));
}
