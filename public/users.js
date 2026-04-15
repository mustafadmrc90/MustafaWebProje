(() => {
  const toggles = Array.from(document.querySelectorAll("[data-user-login-lock-toggle='1']"));
  if (toggles.length === 0) return;

  toggles.forEach((toggle) => {
    toggle.addEventListener("change", () => {
      const form = toggle.closest("form");
      if (!(form instanceof HTMLFormElement)) return;
      form.requestSubmit();
    });
  });
})();
