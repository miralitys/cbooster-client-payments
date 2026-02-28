(function initClientPayment2Page() {
  const frame = document.getElementById("cp2-frame");
  const reloadButton = document.getElementById("cp2-reload-frame");
  const lastCheckedNode = document.getElementById("cp2-last-checked");

  if (!(frame instanceof HTMLIFrameElement)) {
    return;
  }

  let resizeTimer = null;
  let heightPoller = null;

  function renderLastChecked() {
    if (!lastCheckedNode) {
      return;
    }
    const formatter = new Intl.DateTimeFormat("en-US", {
      month: "short",
      day: "2-digit",
      year: "numeric",
      hour: "2-digit",
      minute: "2-digit",
    });
    lastCheckedNode.textContent = formatter.format(new Date());
  }

  function resizeFrame() {
    try {
      const doc = frame.contentDocument;
      if (!doc || !doc.documentElement) {
        return;
      }

      const body = doc.body;
      const root = doc.documentElement;
      const nextHeight = Math.max(
        body ? body.scrollHeight : 0,
        root.scrollHeight,
        body ? body.offsetHeight : 0,
        root.offsetHeight,
        980,
      );
      frame.style.height = String(nextHeight + 24) + "px";
    } catch {
      // Ignore cross-page transitions while frame navigates.
    }
  }

  function injectEmbeddedTheme() {
    try {
      const doc = frame.contentDocument;
      if (!doc || !doc.head || !doc.body) {
        return;
      }

      if (!doc.getElementById("cp2-embedded-theme")) {
        const style = doc.createElement("style");
        style.id = "cp2-embedded-theme";
        style.textContent = [
          "body { background: linear-gradient(180deg, #f2f8ff 0%, #eaf7f2 100%) !important; }",
          ".page-shell > .container { max-width: 100% !important; padding: 16px !important; }",
          ".page-header { display: none !important; }",
          ".section, .cb-panel, .cb-page-header-panel { border-radius: 18px !important; }",
          ".cb-panel, .cb-page-header-panel { border-color: #d3e0ef !important; box-shadow: 0 12px 30px rgba(12, 55, 96, 0.08) !important; }",
          ".section-heading { font-size: 34px !important; letter-spacing: -0.02em !important; }",
          ".table-panel { background: #ffffffee !important; }",
        ].join("\n");
        doc.head.appendChild(style);
      }

      if (doc.body && !doc.body.classList.contains("cp2-embedded-body")) {
        doc.body.classList.add("cp2-embedded-body");
      }
    } catch {
      // Ignore when iframe is still loading or auth redirect happens.
    }
  }

  function onFrameLoaded() {
    if (resizeTimer) {
      window.cancelAnimationFrame(resizeTimer);
      resizeTimer = null;
    }

    injectEmbeddedTheme();
    renderLastChecked();
    resizeFrame();

    resizeTimer = window.requestAnimationFrame(resizeFrame);
  }

  frame.addEventListener("load", onFrameLoaded);

  if (reloadButton instanceof HTMLButtonElement) {
    reloadButton.addEventListener("click", function handleReloadClick() {
      try {
        if (frame.contentWindow) {
          frame.contentWindow.location.reload();
          renderLastChecked();
          return;
        }
      } catch {
        // Fall through to resetting src.
      }

      const source = frame.getAttribute("src") || "/app/client-payments";
      frame.setAttribute("src", source);
      renderLastChecked();
    });
  }

  renderLastChecked();
  resizeFrame();

  heightPoller = window.setInterval(function syncFrameHeight() {
    injectEmbeddedTheme();
    resizeFrame();
  }, 1600);

  window.addEventListener("beforeunload", function cleanup() {
    if (heightPoller) {
      window.clearInterval(heightPoller);
      heightPoller = null;
    }
  });
})();
