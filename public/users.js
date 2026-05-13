(() => {
  const loginLockToggles = Array.from(document.querySelectorAll("[data-user-login-lock-toggle='1']"));
  const ajaxToggles = Array.from(
    document.querySelectorAll("[data-user-allowed-computer-toggle='1'], [data-user-device-permission-toggle='1']")
  );
  const panelButtons = Array.from(document.querySelectorAll("[data-user-device-panel-toggle='1']"));
  const panelRows = Array.from(document.querySelectorAll("[data-user-device-panel]"));
  const feedbackBackdrop = document.querySelector("[data-user-feedback-backdrop]");
  const feedbackMessageEl = document.querySelector("[data-user-feedback-message]");
  const feedbackCloseButton = document.querySelector("[data-user-feedback-close='1']");

  const parseJsonResponse = async (response) => {
    try {
      return await response.json();
    } catch (err) {
      return null;
    }
  };

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

  const closeFeedback = () => {
    if (!(feedbackBackdrop instanceof HTMLElement)) return;
    feedbackBackdrop.hidden = true;
    feedbackBackdrop.classList.remove("active");
    feedbackBackdrop.setAttribute("aria-hidden", "true");
    document.body.classList.remove("user-feedback-open");
  };

  const openFeedback = (message) => {
    if (!(feedbackBackdrop instanceof HTMLElement) || !(feedbackMessageEl instanceof HTMLElement)) return;
    feedbackMessageEl.textContent = String(message || "").trim();
    feedbackBackdrop.hidden = false;
    feedbackBackdrop.classList.add("active");
    feedbackBackdrop.setAttribute("aria-hidden", "false");
    document.body.classList.add("user-feedback-open");
    window.setTimeout(() => {
      feedbackCloseButton?.focus();
    }, 0);
  };

  const buildSuccessMessageForToggle = (toggle, data) => {
    const toggleName = String(toggle?.name || "").trim();
    if (toggleName === "enabled" && toggle.hasAttribute("data-user-allowed-computer-toggle")) {
      return toggle.checked
        ? String(data?.message || "Izinli Bilgisayar aktif edildi. Bu kullanici artik izin verilen cihazlarla giris yapabilir.")
        : "";
    }

    if (toggleName === "ipEnabled") {
      return toggle.checked
        ? "IP adresi aktif edildi. Bu IP adresi ile giris yapilabilir."
        : "";
    }

    if (toggleName === "macEnabled") {
      return toggle.checked
        ? "MAC adresi aktif edildi. Bu MAC adresi ile giris yapilabilir."
        : "";
    }

    return toggle.checked ? String(data?.message || "Ayar guncellendi.") : "";
  };

  const submitAjaxToggle = async (toggle) => {
    const form = toggle.closest("form");
    if (!(form instanceof HTMLFormElement)) return;

    const previousChecked = !toggle.checked;
    const requestBody = new URLSearchParams(new FormData(form));
    const siblingToggles = Array.from(form.querySelectorAll("input[type='checkbox']"));
    siblingToggles.forEach((item) => {
      item.disabled = true;
    });

    try {
      const response = await fetch(form.action, {
        method: "POST",
        headers: {
          Accept: "application/json",
          "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"
        },
        body: requestBody.toString()
      });
      const data = await parseJsonResponse(response);
      if (!response.ok || data?.ok === false) {
        throw new Error(String(data?.error || "Ayar guncellenemedi.").trim() || "Ayar guncellenemedi.");
      }

      const successMessage = buildSuccessMessageForToggle(toggle, data);
      if (successMessage) {
        openFeedback(successMessage);
      }
    } catch (err) {
      toggle.checked = previousChecked;
      openFeedback(String(err?.message || "Ayar guncellenemedi.").trim() || "Ayar guncellenemedi.");
    } finally {
      siblingToggles.forEach((item) => {
        item.disabled = false;
      });
    }
  };

  loginLockToggles.forEach((toggle) => {
    toggle.addEventListener("change", () => {
      const form = toggle.closest("form");
      if (!(form instanceof HTMLFormElement)) return;
      form.requestSubmit();
    });
  });

  ajaxToggles.forEach((toggle) => {
    toggle.addEventListener("change", () => {
      void submitAjaxToggle(toggle);
    });
  });

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

  feedbackCloseButton?.addEventListener("click", closeFeedback);
  feedbackBackdrop?.addEventListener("click", (event) => {
    if (event.target === feedbackBackdrop) {
      closeFeedback();
    }
  });
  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape") {
      closeFeedback();
    }
  });
})();
