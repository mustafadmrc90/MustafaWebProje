(() => {
  const toggles = Array.from(
    document.querySelectorAll(
      "[data-user-login-lock-toggle='1'], [data-user-allowed-computer-toggle='1'], [data-user-device-permission-toggle='1']"
    )
  );
  const panelButtons = Array.from(document.querySelectorAll("[data-user-device-panel-toggle='1']"));
  const panelRows = Array.from(document.querySelectorAll("[data-user-device-panel]"));

  toggles.forEach((toggle) => {
    toggle.addEventListener("change", () => {
      const form = toggle.closest("form");
      if (!(form instanceof HTMLFormElement)) return;
      form.requestSubmit();
    });
  });

  const setPanelVisibility = (targetId, visible) => {
    panelRows.forEach((row) => {
      const matches = String(row.getAttribute("data-user-device-panel") || "") === String(targetId || "");
      row.hidden = matches ? !visible : true;
    });

    panelButtons.forEach((button) => {
      const matches = String(button.getAttribute("data-user-device-panel-target") || "") === String(targetId || "");
      button.setAttribute("aria-expanded", matches && visible ? "true" : "false");
    });
  };

  panelButtons.forEach((button) => {
    button.addEventListener("click", () => {
      const targetId = String(button.getAttribute("data-user-device-panel-target") || "").trim();
      if (!targetId) return;
      const targetRow = panelRows.find(
        (row) => String(row.getAttribute("data-user-device-panel") || "") === targetId
      );
      const nextVisible = targetRow ? targetRow.hidden : true;
      setPanelVisibility(targetId, nextVisible);
    });
  });
})();
