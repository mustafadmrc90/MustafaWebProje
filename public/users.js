(() => {
  const loginLockToggles = Array.from(document.querySelectorAll("[data-user-login-lock-toggle='1']"));
  const ajaxToggles = Array.from(
    document.querySelectorAll("[data-user-device-approval-required-toggle='1'], [data-user-device-permission-toggle='1']")
  );
  const panelButtons = Array.from(document.querySelectorAll("[data-user-device-panel-toggle='1']"));
  const panelRows = Array.from(document.querySelectorAll("[data-user-device-panel]"));
  const feedbackBackdrop = document.querySelector("[data-user-feedback-backdrop]");
  const feedbackTitleEl = document.querySelector("#user-feedback-title");
  const feedbackMessageEl = document.querySelector("[data-user-feedback-message]");
  const feedbackCloseButton = document.querySelector("[data-user-feedback-close='1']");
  const preserveScrollLinks = Array.from(document.querySelectorAll("[data-user-preserve-scroll='1']"));
  const preserveScrollForms = Array.from(document.querySelectorAll("[data-user-preserve-scroll-form='1']"));
  const userScrollStorageKey = "users_page_scroll_y_v1";

  const parseJsonResponse = async (response) => {
    try {
      return await response.json();
    } catch (err) {
      return null;
    }
  };

  const restoreStoredScrollPosition = () => {
    let storedValue = "";
    try {
      storedValue = window.sessionStorage.getItem(userScrollStorageKey) || "";
      window.sessionStorage.removeItem(userScrollStorageKey);
    } catch (err) {
      return;
    }

    const scrollY = Number.parseInt(storedValue, 10);
    if (!Number.isFinite(scrollY) || scrollY < 0) return;
    window.requestAnimationFrame(() => {
      window.scrollTo({ top: scrollY, left: 0, behavior: "auto" });
    });
  };

  const storeCurrentScrollPosition = () => {
    try {
      window.sessionStorage.setItem(userScrollStorageKey, String(Math.max(0, Math.round(window.scrollY || 0))));
    } catch (err) {
      // Ignore storage errors.
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

  const openFeedback = (message, { title = "Bilgi" } = {}) => {
    const normalizedMessage = String(message || "").trim();
    if (!normalizedMessage) return;

    if (!(feedbackBackdrop instanceof HTMLElement) || !(feedbackMessageEl instanceof HTMLElement)) {
      window.alert(normalizedMessage);
      return;
    }

    if (feedbackTitleEl instanceof HTMLElement) {
      feedbackTitleEl.textContent = String(title || "Bilgi").trim() || "Bilgi";
    }

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
    const responseMessage = String(data?.message || "").trim();
    const toggleName = String(toggle?.name || "").trim();
    if (toggleName === "enabled" && toggle.hasAttribute("data-user-device-approval-required-toggle")) {
      return (
        responseMessage ||
        (toggle.checked
          ? "Cihaz Onayi aktif edildi. Bu kullanici artik sadece Cihazlar bolumunden onay verilen IP ve MAC kayitlariyla giris yapabilir."
          : "Cihaz Onayi kapatildi. Bu kullanici yeniden cihaz onayi olmadan giris yapabilir.")
      );
    }

    if (toggleName === "approved") {
      return (
        responseMessage ||
        (toggle.checked
          ? "Cihaza onay verildi. Bu IP ve MAC kaydiyla giris yapilabilir."
          : "Cihaz onayi kaldirildi. Bu kayit ile giris engellenecek.")
      );
    }

    return responseMessage || "Ayar guncellendi.";
  };

  const syncApprovedDeviceUi = (form, approved) => {
    const normalizedApproved = Boolean(approved);
    const approvedLabel = form.querySelector("[data-user-device-approved-label='1']");
    if (approvedLabel instanceof HTMLElement) {
      approvedLabel.textContent = normalizedApproved ? "Onaylı" : "Onay Bekliyor";
      approvedLabel.classList.toggle("muted", !normalizedApproved);
    }

    const panel = form.closest("[data-user-device-panel]");
    if (!(panel instanceof HTMLElement)) return;
    const approvedCountEl = panel.querySelector("[data-user-approved-count='1']");
    if (!(approvedCountEl instanceof HTMLElement)) return;

    const approvedCount = Array.from(
      panel.querySelectorAll("[data-user-device-permission-toggle='1']")
    ).filter((input) => input instanceof HTMLInputElement && input.checked).length;
    approvedCountEl.textContent = `${approvedCount} onaylı`;
  };

  const syncApprovedDeviceForms = (form, approved, updatedDeviceIds = [], approvedCount = null) => {
    const panel = form.closest("[data-user-device-panel]");
    const normalizedIds = Array.isArray(updatedDeviceIds)
      ? updatedDeviceIds.map((item) => String(item || "").trim()).filter(Boolean)
      : [];

    if (!(panel instanceof HTMLElement) || normalizedIds.length === 0) {
      syncApprovedDeviceUi(form, approved);
      if (panel instanceof HTMLElement && approvedCount !== null) {
        const approvedCountEl = panel.querySelector("[data-user-approved-count='1']");
        if (approvedCountEl instanceof HTMLElement) {
          approvedCountEl.textContent = `${Number(approvedCount) || 0} onaylı`;
        }
      }
      return;
    }

    normalizedIds.forEach((deviceId) => {
      const relatedForm = panel.querySelector(`[data-user-device-id="${deviceId}"]`);
      if (!(relatedForm instanceof HTMLFormElement)) return;
      const checkbox = relatedForm.querySelector("[data-user-device-permission-toggle='1']");
      if (checkbox instanceof HTMLInputElement) {
        checkbox.checked = Boolean(approved);
      }
      syncApprovedDeviceUi(relatedForm, approved);
    });

    if (approvedCount !== null) {
      const approvedCountEl = panel.querySelector("[data-user-approved-count='1']");
      if (approvedCountEl instanceof HTMLElement) {
        approvedCountEl.textContent = `${Number(approvedCount) || 0} onaylı`;
      }
    }
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

      if (String(toggle?.name || "").trim() === "approved") {
        syncApprovedDeviceForms(form, data?.approved ?? toggle.checked, data?.updatedDeviceIds, data?.approvedCount);
      }

      const successMessage = buildSuccessMessageForToggle(toggle, data);
      if (successMessage) {
        openFeedback(successMessage, { title: "Bilgi" });
      }
    } catch (err) {
      toggle.checked = previousChecked;
      openFeedback(String(err?.message || "Ayar guncellenemedi.").trim() || "Ayar guncellenemedi.", {
        title: "Uyari"
      });
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

  preserveScrollLinks.forEach((link) => {
    link.addEventListener("click", (event) => {
      if (event.defaultPrevented || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return;
      storeCurrentScrollPosition();
    });
  });

  preserveScrollForms.forEach((form) => {
    form.addEventListener("submit", () => {
      storeCurrentScrollPosition();
    });
  });

  restoreStoredScrollPosition();

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
