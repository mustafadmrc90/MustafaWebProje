(() => {
  const checkbox = document.querySelector("#remember-username");
  const usernameInput = document.querySelector('input[name="username"]');
  const passwordInput = document.querySelector('input[name="password"]');

  if (!checkbox || !usernameInput || !passwordInput) return;

  const savedUsername = localStorage.getItem("rememberedUsername");
  const savedPassword = localStorage.getItem("rememberedPassword");
  if (savedUsername) {
    usernameInput.value = savedUsername;
    checkbox.checked = true;
  }
  if (savedPassword) {
    passwordInput.value = savedPassword;
  }

  const form = document.querySelector("form.auth-form");
  form?.addEventListener("submit", () => {
    if (checkbox.checked) {
      localStorage.setItem("rememberedUsername", usernameInput.value.trim());
      localStorage.setItem("rememberedPassword", passwordInput.value);
    } else {
      localStorage.removeItem("rememberedUsername");
      localStorage.removeItem("rememberedPassword");
    }
  });
})();
