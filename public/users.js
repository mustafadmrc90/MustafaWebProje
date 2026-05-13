(() => {
  const loginLockToggles = Array.from(document.querySelectorAll("[data-user-login-lock-toggle='1']"));
  const ajaxToggles = Array.from(
    document.querySelectorAll("[data-user-allowed-computer-toggle='1'], [data-user-device-permission-toggle='1']")
  );
  const panelButtons = Array.from(document.querySelectorAll("[data-user-device-panel-toggle='1']"));
  const panelRows = Array.from(document.querySelectorAll("[data-user-device-panel]"));

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

  const openFeedback = (message) => {
    const normalizedMessage = String(message || "").trim();
    if (!normalizedMessage) return;
    window.alert(normalizedMessage);
  };

  const buildSuccessMessageForToggle = (toggle, data) => {
    const responseMessage = String(data?.message || "").trim();
    const toggleName = String(toggle?.name || "").trim();
    if (toggleName === "enabled" && toggle.hasAttribute("data-user-allowed-computer-toggle")) {
      return (
        responseMessage ||
        (toggle.checked
          ? "Izinli Bilgisayar aktif edildi. Bu kullanici artik sadece izin verilen cihazlarla giris yapabilir."
          : "Izinli Bilgisayar kapatildi. Bu kullanici yeniden tum cihazlardan giris yapabilir.")
      );
    }

    if (toggleName === "ipEnabled") {
      return (
        responseMessage ||
        (toggle.checked
          ? "IP adresi aktif edildi. Bu IP adresi ile giris yapilabilir."
          : "IP adresi izni kapatildi.")
      );
    }

    if (toggleName === "macEnabled") {
      return (
        responseMessage ||
        (toggle.checked
          ? "MAC adresi aktif edildi. Bu MAC adresi ile giris yapilabilir."
          : "MAC adresi izni kapatildi.")
      );
    }

    return responseMessage || "Ayar guncellendi.";
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
})();
