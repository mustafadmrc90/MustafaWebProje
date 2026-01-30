(() => {
  const checkbox = document.querySelector("#remember-username");
  const usernameInput = document.querySelector('input[name="username"]');

  if (!checkbox || !usernameInput) return;

  const saved = localStorage.getItem("rememberedUsername");
  if (saved) {
    usernameInput.value = saved;
    checkbox.checked = true;
  }

  const form = document.querySelector("form.auth-form");
  form?.addEventListener("submit", () => {
    if (checkbox.checked) {
      localStorage.setItem("rememberedUsername", usernameInput.value.trim());
    } else {
      localStorage.removeItem("rememberedUsername");
    }
  });
})();
