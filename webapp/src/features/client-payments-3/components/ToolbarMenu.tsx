import { useCallback, useEffect, useRef, useState } from "react";

import { Button, Select } from "@/shared/ui";

export type GridDensity = "compact" | "regular";

type MenuKey = "export" | "refresh";

interface ToolbarMenuProps {
  isRefreshing: boolean;
  canManageRefreshActions: boolean;
  isManagerRefreshLoading: boolean;
  isPhonesRefreshLoading: boolean;
  showAllPayments: boolean;
  density: GridDensity;
  lastSyncedLabel: string;
  onRefreshData: () => void;
  onExportXls: () => void;
  onExportPdf: () => void;
  onToggleShowAllPayments: () => void;
  onRefreshManager: () => void;
  onTotalRefreshManager: () => void;
  onRefreshPhones: () => void;
  onDensityChange: (density: GridDensity) => void;
}

export function ToolbarMenu({
  isRefreshing,
  canManageRefreshActions,
  isManagerRefreshLoading,
  isPhonesRefreshLoading,
  showAllPayments,
  density,
  lastSyncedLabel,
  onRefreshData,
  onExportXls,
  onExportPdf,
  onToggleShowAllPayments,
  onRefreshManager,
  onTotalRefreshManager,
  onRefreshPhones,
  onDensityChange,
}: ToolbarMenuProps) {
  const [exportMenuOpen, setExportMenuOpen] = useState(false);
  const [refreshMenuOpen, setRefreshMenuOpen] = useState(false);

  const exportRootRef = useRef<HTMLDivElement | null>(null);
  const refreshRootRef = useRef<HTMLDivElement | null>(null);

  const activeMenu: MenuKey | null = exportMenuOpen ? "export" : refreshMenuOpen ? "refresh" : null;
  const isBusy = isManagerRefreshLoading || isPhonesRefreshLoading;

  const focusToggleButton = useCallback((menu: MenuKey) => {
    const selector = menu === "export" ? "[data-cp3-export-toggle='1']" : "[data-cp3-refresh-toggle='1']";
    const root = menu === "export" ? exportRootRef.current : refreshRootRef.current;
    const toggle = root?.querySelector(selector);
    if (toggle instanceof HTMLButtonElement) {
      toggle.focus();
    }
  }, []);

  const focusFirstMenuItem = useCallback((menu: MenuKey) => {
    const root = menu === "export" ? exportRootRef.current : refreshRootRef.current;
    const firstItem = root?.querySelector(".cp3-toolbar-menu__item:not(:disabled)");
    if (firstItem instanceof HTMLButtonElement) {
      firstItem.focus();
    }
  }, []);

  const closeMenus = useCallback((menuToFocus?: MenuKey) => {
    setExportMenuOpen(false);
    setRefreshMenuOpen(false);
    if (menuToFocus) {
      window.requestAnimationFrame(() => {
        focusToggleButton(menuToFocus);
      });
    }
  }, [focusToggleButton]);

  useEffect(() => {
    if (exportMenuOpen) {
      focusFirstMenuItem("export");
    }
  }, [exportMenuOpen, focusFirstMenuItem]);

  useEffect(() => {
    if (refreshMenuOpen) {
      focusFirstMenuItem("refresh");
    }
  }, [focusFirstMenuItem, refreshMenuOpen]);

  useEffect(() => {
    if (!activeMenu) {
      return;
    }

    function onPointerDown(event: MouseEvent) {
      const target = event.target;
      if (!(target instanceof Node)) {
        return;
      }

      const clickedInsideExport = Boolean(exportRootRef.current?.contains(target));
      const clickedInsideRefresh = Boolean(refreshRootRef.current?.contains(target));
      if (!clickedInsideExport && !clickedInsideRefresh) {
        if (activeMenu) {
          closeMenus(activeMenu);
        }
      }
    }

    function onKeyDown(event: KeyboardEvent) {
      if (event.key === "Escape") {
        event.preventDefault();
        if (activeMenu) {
          closeMenus(activeMenu);
        }
        return;
      }

      if (event.key !== "Tab") {
        return;
      }

      const root = activeMenu === "export" ? exportRootRef.current : refreshRootRef.current;
      const panel = root?.querySelector(".cp3-toolbar-menu__panel");
      if (!(panel instanceof HTMLElement)) {
        return;
      }

      const focusableElements = Array.from(
        panel.querySelectorAll<HTMLElement>(
          "button:not([disabled]), [href], input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex='-1'])",
        ),
      ).filter((element) => !element.hasAttribute("hidden"));

      if (!focusableElements.length) {
        return;
      }

      const first = focusableElements[0];
      const last = focusableElements[focusableElements.length - 1];
      const activeElement = document.activeElement;

      if (event.shiftKey && activeElement === first) {
        event.preventDefault();
        last.focus();
      }

      if (!event.shiftKey && activeElement === last) {
        event.preventDefault();
        first.focus();
      }
    }

    document.addEventListener("mousedown", onPointerDown);
    document.addEventListener("keydown", onKeyDown);
    return () => {
      document.removeEventListener("mousedown", onPointerDown);
      document.removeEventListener("keydown", onKeyDown);
    };
  }, [activeMenu, closeMenus]);

  return (
    <section className="cp3-toolbar" aria-label="Панель действий таблицы">
      <div className="cp3-toolbar__left">
        <Button type="button" variant="secondary" size="sm" onClick={onRefreshData} isLoading={isRefreshing}>
          Обновить данные
        </Button>
        <p className="cp3-toolbar__sync">Последняя синхронизация: {lastSyncedLabel}</p>
      </div>

      <div className="cp3-toolbar__right">
        <label className="cp3-toolbar__density" htmlFor="cp3-density-select">
          <span>Плотность</span>
          <Select
            id="cp3-density-select"
            value={density}
            onChange={(event) => onDensityChange((event.target.value as GridDensity) || "compact")}
          >
            <option value="compact">Компактный</option>
            <option value="regular">Обычный</option>
          </Select>
        </label>

        <Button type="button" variant="secondary" size="sm" onClick={onToggleShowAllPayments}>
          {showAllPayments ? "Скрыть доп. платежи" : "Показать все платежи"}
        </Button>

        <div ref={exportRootRef} className="cp3-toolbar-menu">
          <Button
            data-cp3-export-toggle="1"
            type="button"
            variant="secondary"
            size="sm"
            aria-haspopup="menu"
            aria-expanded={exportMenuOpen}
            aria-controls="cp3-export-menu-panel"
            onClick={() => {
              setExportMenuOpen((prev) => !prev);
              setRefreshMenuOpen(false);
            }}
          >
            Экспорт {exportMenuOpen ? "▴" : "▾"}
          </Button>

          <div
            id="cp3-export-menu-panel"
            role="menu"
            className="cp3-toolbar-menu__panel"
            hidden={!exportMenuOpen}
            aria-label="Меню экспорта"
          >
            <button
              type="button"
              role="menuitem"
              className="cp3-toolbar-menu__item"
              onClick={() => {
                closeMenus("export");
                onExportXls();
              }}
            >
              Экспорт в Excel (.xlsx)
            </button>

            <button
              type="button"
              role="menuitem"
              className="cp3-toolbar-menu__item"
              onClick={() => {
                closeMenus("export");
                onExportPdf();
              }}
            >
              Экспорт в PDF
            </button>
          </div>
        </div>

        {canManageRefreshActions ? (
          <div ref={refreshRootRef} className="cp3-toolbar-menu">
            <Button
              data-cp3-refresh-toggle="1"
              type="button"
              variant="secondary"
              size="sm"
              aria-haspopup="menu"
              aria-expanded={refreshMenuOpen}
              aria-controls="cp3-refresh-menu-panel"
              onClick={() => {
                setRefreshMenuOpen((prev) => !prev);
                setExportMenuOpen(false);
              }}
              isLoading={isBusy}
            >
              Refresh {refreshMenuOpen ? "▴" : "▾"}
            </Button>

            <div
              id="cp3-refresh-menu-panel"
              role="menu"
              className="cp3-toolbar-menu__panel"
              hidden={!refreshMenuOpen}
              aria-label="Меню refresh"
            >
              <button
                type="button"
                role="menuitem"
                className="cp3-toolbar-menu__item"
                disabled={isBusy}
                onClick={() => {
                  closeMenus("refresh");
                  onRefreshManager();
                }}
              >
                Refresh Manager
              </button>
              <button
                type="button"
                role="menuitem"
                className="cp3-toolbar-menu__item"
                disabled={isBusy}
                onClick={() => {
                  closeMenus("refresh");
                  onTotalRefreshManager();
                }}
              >
                Total Refresh Manager
              </button>
              <button
                type="button"
                role="menuitem"
                className="cp3-toolbar-menu__item"
                disabled={isBusy}
                onClick={() => {
                  closeMenus("refresh");
                  onRefreshPhones();
                }}
              >
                Добавить/Рефреш телефонов
              </button>
            </div>
          </div>
        ) : null}
      </div>
    </section>
  );
}
