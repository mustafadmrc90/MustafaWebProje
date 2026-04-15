(() => {
  const checkbox = document.querySelector("#remember-username");
  const usernameInput = document.querySelector('input[name="username"]');
  const passwordInput = document.querySelector('input[name="password"]');
  const form = document.querySelector("form.auth-form[data-login-form='1']");
  const submitButton = form?.querySelector("button[type='submit']");
  const errorEl = document.querySelector("[data-login-error]");
  const lockNoteEl = document.querySelector("[data-login-lock-note]");
  const lockedProfileStorageKey = "obus_locked_login_profile_v1";

  if (!checkbox || !usernameInput || !passwordInput || !form) return;

  const setError = (message) => {
    const normalizedMessage = String(message || "").trim();
    if (!errorEl) return;
    errorEl.textContent = normalizedMessage;
    errorEl.hidden = !normalizedMessage;
  };

  const setLockedState = (locked, message = "") => {
    const isLocked = Boolean(locked);
    usernameInput.readOnly = isLocked;
    passwordInput.readOnly = isLocked;
    checkbox.disabled = isLocked;
    if (isLocked) {
      checkbox.checked = true;
    }
    if (lockNoteEl) {
      lockNoteEl.textContent = String(message || "").trim();
      lockNoteEl.hidden = !String(message || "").trim();
    }
  };

  const clearLockedProfile = () => {
    window.localStorage.removeItem(lockedProfileStorageKey);
    setLockedState(false, "");
  };

  const parseLockedProfile = () => {
    try {
      const raw = window.localStorage.getItem(lockedProfileStorageKey);
      if (!raw) return null;
      const parsed = JSON.parse(raw);
      const username = String(parsed?.username || "").trim();
      const password = typeof parsed?.password === "string" ? parsed.password : "";
      const version = Number(parsed?.version);
      if (!username || !password || !Number.isInteger(version)) return null;
      return {
        username,
        password,
        version
      };
    } catch (err) {
      return null;
    }
  };

  const storeLockedProfile = ({ username, password, version }) => {
    const normalizedUsername = String(username || "").trim();
    const normalizedPassword = typeof password === "string" ? password : "";
    const normalizedVersion = Number(version);
    if (!normalizedUsername || !normalizedPassword || !Number.isInteger(normalizedVersion)) {
      clearLockedProfile();
      return;
    }
    window.localStorage.setItem(
      lockedProfileStorageKey,
      JSON.stringify({
        username: normalizedUsername,
        password: normalizedPassword,
        version: normalizedVersion
      })
    );
  };

  const parseJsonResponse = async (response) => {
    try {
      return await response.json();
    } catch (err) {
      return null;
    }
  };

  const savedUsername = localStorage.getItem("rememberedUsername");
  const savedPassword = localStorage.getItem("rememberedPassword");
  if (savedUsername) {
    usernameInput.value = savedUsername;
    checkbox.checked = true;
  }
  if (savedPassword) {
    passwordInput.value = savedPassword;
  }

  const syncRememberedCredentials = () => {
    if (checkbox.checked) {
      localStorage.setItem("rememberedUsername", usernameInput.value.trim());
      localStorage.setItem("rememberedPassword", passwordInput.value);
    } else {
      localStorage.removeItem("rememberedUsername");
      localStorage.removeItem("rememberedPassword");
    }
  };

  const applyLockedProfileIfAvailable = async () => {
    const lockedProfile = parseLockedProfile();
    if (!lockedProfile) return;

    try {
      const response = await fetch(`/api/login-lock-status?username=${encodeURIComponent(lockedProfile.username)}`, {
        headers: {
          Accept: "application/json"
        }
      });
      const data = await parseJsonResponse(response);
      const version = Number(data?.version);
      const isStillLocked =
        response.ok &&
        data?.ok !== false &&
        data?.enabled === true &&
        Number.isInteger(version) &&
        version === lockedProfile.version &&
        String(data?.username || "").trim() === lockedProfile.username;

      if (!isStillLocked) {
        clearLockedProfile();
        return;
      }

      usernameInput.value = lockedProfile.username;
      passwordInput.value = lockedProfile.password;
      setLockedState(true, "Bu kullanıcı için giriş alanları sabitlendi.");
    } catch (err) {
      clearLockedProfile();
    }
  };

  void applyLockedProfileIfAvailable();

  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    setError("");

    const formData = new URLSearchParams(new FormData(form));
    const currentPassword = String(passwordInput.value || "");

    if (submitButton) {
      submitButton.disabled = true;
    }

    try {
      const response = await fetch(form.action, {
        method: "POST",
        headers: {
          Accept: "application/json",
          "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"
        },
        body: formData.toString()
      });
      const data = await parseJsonResponse(response);

      if (!response.ok || data?.ok === false) {
        setError(String(data?.error || "Hatalı giriş.").trim() || "Hatalı giriş.");
        return;
      }

      syncRememberedCredentials();

      const loginLock = data?.loginLock;
      if (loginLock?.enabled === true) {
        storeLockedProfile({
          username: String(loginLock?.username || usernameInput.value || "").trim(),
          password: currentPassword,
          version: Number(loginLock?.version)
        });
      } else {
        clearLockedProfile();
      }

      window.location.assign(String(data?.redirectTo || "/dashboard"));
    } catch (err) {
      setError(String(err?.message || "Giriş işlemi başarısız.").trim() || "Giriş işlemi başarısız.");
    } finally {
      if (submitButton) {
        submitButton.disabled = false;
      }
    }
  });
})();
