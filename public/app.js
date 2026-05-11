(() => {
  const sidebar = document.querySelector(".sidebar");
  const content = document.querySelector(".content");
  const sidebarCollapsedStorageKey = "dashboard_sidebar_collapsed_v1";

  if (!sidebar || !content) return;

  const loadSidebarCollapsedState = () => {
    try {
      return window.localStorage.getItem(sidebarCollapsedStorageKey) === "true";
    } catch (err) {
      return false;
    }
  };

  const saveSidebarCollapsedState = (collapsed) => {
    try {
      window.localStorage.setItem(sidebarCollapsedStorageKey, collapsed ? "true" : "false");
      return true;
    } catch (err) {
      return false;
    }
  };

  const applySidebarState = (collapsed) => {
    document.body.classList.toggle("sidebar-collapsed", collapsed);
    const toggle = document.querySelector("[data-sidebar-toggle]");
    if (toggle) {
      const label = collapsed ? "Menüyü genişlet" : "Menüyü daralt";
      toggle.setAttribute("aria-label", label);
      toggle.setAttribute("title", label);
    }
    if (collapsed) {
      document.querySelectorAll(".nav-accordion").forEach((section) => {
        section.setAttribute("open", "");
      });
    }
  };

  const isModified = (event) =>
    event.metaKey || event.ctrlKey || event.shiftKey || event.altKey;

  const normalizePath = (value) => {
    try {
      const parsed = new URL(value, window.location.origin);
      const normalized = parsed.pathname.replace(/\/+$/, "");
      return normalized || "/";
    } catch (err) {
      return "/";
    }
  };

  const setActive = (path) => {
    const targetPath = normalizePath(path);
    document.querySelectorAll(".nav-item").forEach((item) => {
      const hrefPath = normalizePath(item.getAttribute("href") || "/");
      item.classList.toggle("active", hrefPath === targetPath);
    });
    document.querySelectorAll(".nav-accordion").forEach((section) => {
      if (section.querySelector(".nav-item.active")) {
        section.setAttribute("open", "");
      }
    });
  };

  const replaceContent = (html, url, push = true) => {
    const doc = new DOMParser().parseFromString(html, "text/html");
    const next = doc.querySelector(".content");
    if (!next) return false;

    document.body.classList.remove("screen-log-modal-open");

    const nextSidebar = doc.querySelector(".sidebar");
    if (nextSidebar) {
      // Keep the same sidebar element so delegated click handlers remain attached.
      sidebar.innerHTML = nextSidebar.innerHTML;
    }

    content.innerHTML = next.innerHTML;
    document.title = doc.title || document.title;
    setActive(new URL(url).pathname);
    applySidebarState(loadSidebarCollapsedState());
    initEndpointUI();

    if (push) {
      history.pushState({}, "", url);
    }

    window.scrollTo({ top: 0, behavior: "auto" });
    return true;
  };

  const navigate = async (url, { push } = { push: true }) => {
    try {
      const response = await fetch(url, {
        headers: { "X-Requested-With": "dashboard" }
      });
      if (!response.ok) {
        window.location.href = url;
        return;
      }
      const html = await response.text();
      if (!replaceContent(html, url, push)) {
        window.location.href = url;
      }
    } catch (err) {
      window.location.href = url;
    }
  };

  sidebar.addEventListener("click", (event) => {
    const toggle = event.target.closest("[data-sidebar-toggle]");
    if (toggle) {
      event.preventDefault();
      const nextCollapsed = !document.body.classList.contains("sidebar-collapsed");
      saveSidebarCollapsedState(nextCollapsed);
      applySidebarState(nextCollapsed);
      return;
    }

    const link = event.target.closest("a.nav-item");
    if (!link) return;
    if (link.target || isModified(event)) return;
    event.preventDefault();
    navigate(link.href);
  });

  window.addEventListener("popstate", () => {
    navigate(window.location.href, { push: false });
  });

  applySidebarState(loadSidebarCollapsedState());

  const parseJsonResponse = async (response) => {
    const contentType = response.headers.get("content-type") || "";
    if (!contentType.includes("application/json")) return null;
    try {
      return await response.json();
    } catch (err) {
      return null;
    }
  };

  const getPayloadTooLargeMessage = (fallback) => {
    const actionText = String(fallback || "").trim().toLocaleLowerCase("tr");
    if (actionText.includes("toplu kullanıcı oluşturma")) {
      return "Toplu kullanıcı oluşturma isteği çok büyük. Çok fazla firma veya kullanıcı seçildiği için gönderilen veri sınırı aşıldı.";
    }
    if (actionText.includes("toplu kullanıcı sorgusu")) {
      return "Toplu kullanıcı sorgu isteği çok büyük. Girilen kullanıcı veya firma sayısı nedeniyle gönderilen veri sınırı aşıldı.";
    }
    return "İstek çok büyük. Gönderilen veri sunucu sınırını aştı.";
  };

  const getApiErrorMessage = (response, data, fallback) => {
    if (data?.error) return data.error;
    if (response?.status === 413) {
      return getPayloadTooLargeMessage(fallback);
    }
    if (response?.status === 401) {
      return "Oturum süresi doldu. Lütfen tekrar giriş yapın.";
    }
    if (response?.redirected && /\/login(?:$|[?#])/.test(response.url || "")) {
      return "Oturum süresi doldu. Lütfen tekrar giriş yapın.";
    }
    return `${fallback} (${response?.status || 0})`;
  };

  const loadEndpoints = async () => {
    try {
      const response = await fetch("/api/endpoints", {
        headers: { Accept: "application/json" }
      });
      const data = await parseJsonResponse(response);
      if (!response.ok || !data?.ok || !Array.isArray(data.items)) {
        return [];
      }
      return data.items;
    } catch (err) {
      return [];
    }
  };

  const loadTargetUrls = async () => {
    try {
      const response = await fetch("/api/targets", {
        headers: { Accept: "application/json" }
      });
      const data = await parseJsonResponse(response);
      if (!response.ok || !data?.ok || !Array.isArray(data.items)) {
        return [];
      }
      return data.items;
    } catch (err) {
      return [];
    }
  };

  const saveTargetUrlRecord = async (url) => {
    try {
      const response = await fetch("/api/targets", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Accept: "application/json"
        },
        body: JSON.stringify({ url })
      });
      const data = await parseJsonResponse(response);
      if (!response.ok || data?.ok === false) {
        return {
          item: null,
          error: getApiErrorMessage(response, data, "Hedef URL kaydedilemedi")
        };
      }
      if (!data?.item) {
        return {
          item: null,
          error: "Sunucudan geçerli hedef URL kaydı alınamadı."
        };
      }
      return {
        item: data.item,
        error: null
      };
    } catch (err) {
      return {
        item: null,
        error: err.message || "Hedef URL kaydedilemedi."
      };
    }
  };

  const saveEndpoint = async (payload) => {
    try {
      const response = await fetch("/api/endpoints", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Accept: "application/json"
        },
        body: JSON.stringify(payload)
      });
      const data = await parseJsonResponse(response);
      if (!response.ok || data?.ok === false) {
        return {
          item: null,
          error: getApiErrorMessage(response, data, "Kaydetme hatası")
        };
      }
      if (!data?.item) {
        return {
          item: null,
          error: "Sunucudan geçerli kayıt yanıtı alınamadı."
        };
      }
      return {
        item: data.item,
        error: null
      };
    } catch (err) {
      return {
        item: null,
        error: err.message || "İstek gönderilemedi."
      };
    }
  };

  const loadRequests = async (endpointId) => {
    if (!Number.isInteger(Number(endpointId))) return [];
    try {
      const response = await fetch(`/api/requests/${endpointId}`);
      if (!response.ok) return [];
      const data = await parseJsonResponse(response);
      return data?.items || [];
    } catch (err) {
      return [];
    }
  };

  const loadRequestDetail = async (id) => {
    if (!Number.isInteger(Number(id))) return null;
    try {
      const response = await fetch(`/api/requests/item/${id}`);
      if (!response.ok) return null;
      const data = await parseJsonResponse(response);
      return data?.item || null;
    } catch (err) {
      return null;
    }
  };

  const clearRequests = async (endpointId) => {
    if (!Number.isInteger(Number(endpointId))) return false;
    try {
      const response = await fetch(`/api/requests/${endpointId}`, { method: "DELETE" });
      return response.ok;
    } catch (err) {
      return false;
    }
  };

  const updateEndpoint = async (id, payload) => {
    if (!Number.isInteger(Number(id))) return false;
    try {
      const response = await fetch(`/api/endpoints/${id}`, {
        method: "PUT",
        headers: {
          "Content-Type": "application/json",
          Accept: "application/json"
        },
        body: JSON.stringify(payload)
      });
      if (!response.ok) return false;
      const data = await parseJsonResponse(response);
      if (data && data.ok === false) return false;
      return true;
    } catch (err) {
      return false;
    }
  };

  const reorderEndpointList = async (orderedIds) => {
    const ids = Array.isArray(orderedIds)
      ? orderedIds.map((value) => Number(value)).filter((id) => Number.isInteger(id))
      : [];
    if (!ids.length) {
      return { ok: false, items: [], error: "Sıralama listesi boş." };
    }
    try {
      const response = await fetch("/api/endpoints/reorder", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Accept: "application/json"
        },
        body: JSON.stringify({ ids })
      });
      const data = await parseJsonResponse(response);
      if (!response.ok || data?.ok === false) {
        return {
          ok: false,
          items: [],
          error: getApiErrorMessage(response, data, "Sıralama kaydedilemedi")
        };
      }
      return {
        ok: true,
        items: Array.isArray(data?.items) ? data.items : [],
        error: null
      };
    } catch (err) {
      return {
        ok: false,
        items: [],
        error: err.message || "Sıralama kaydedilemedi."
      };
    }
  };

  const defaultBody = `{\n  \"type\": 1,\n  \"connection\": {\n    \"ip-address\": \"212.156.219.182\",\n    \"port\": \"5117\"\n  },\n  \"browser\": {\n    \"name\": \"Chrome\"\n  }\n}`;
  const defaultHeaders = "{\n  \"Content-Type\": \"application/json\"\n}";
  const defaultParams = "{}";
  const loginProfilesStorageKey = "obus_userlogin_profiles_v1";
  const selectedTargetUrlStorageKey = "obus_selected_target_url_v1";
  const endpointLastResponsesStorageKey = "obus_endpoint_last_responses_v1";
  const mentiChatGptChatStorageKey = "menti_chatgpt_chat_state_v1";
  const mentiChatGptChatRuntime = {
    cleanup: null
  };

  const loadLoginProfilesFromStorage = () => {
    try {
      const raw = window.localStorage.getItem(loginProfilesStorageKey);
      if (!raw) return [];
      const parsed = JSON.parse(raw);
      if (!Array.isArray(parsed)) return [];
      return parsed
        .map((item) => ({
          id: String(item?.id || "").trim(),
          name: String(item?.name || "").trim(),
          partnerCode: String(item?.partnerCode || "").trim(),
          branchId: String(item?.branchId || "").trim()
        }))
        .filter((item) => item.id);
    } catch (err) {
      return [];
    }
  };

  const saveLoginProfilesToStorage = (profiles) => {
    try {
      window.localStorage.setItem(loginProfilesStorageKey, JSON.stringify(profiles || []));
      return true;
    } catch (err) {
      return false;
    }
  };

  const loadSelectedTargetUrlFromStorage = () => {
    try {
      return String(window.localStorage.getItem(selectedTargetUrlStorageKey) || "").trim();
    } catch (err) {
      return "";
    }
  };

  const saveSelectedTargetUrlToStorage = (url) => {
    try {
      const value = String(url || "").trim();
      if (value) {
        window.localStorage.setItem(selectedTargetUrlStorageKey, value);
      } else {
        window.localStorage.removeItem(selectedTargetUrlStorageKey);
      }
      return true;
    } catch (err) {
      return false;
    }
  };

  const loadEndpointLastResponsesFromStorage = () => {
    try {
      const raw = window.localStorage.getItem(endpointLastResponsesStorageKey);
      if (!raw) return {};
      const parsed = JSON.parse(raw);
      if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
        return {};
      }
      return parsed;
    } catch (err) {
      return {};
    }
  };

  const saveEndpointLastResponsesToStorage = (payload) => {
    try {
      window.localStorage.setItem(endpointLastResponsesStorageKey, JSON.stringify(payload || {}));
      return true;
    } catch (err) {
      return false;
    }
  };

  const normalizeEndpoints = (items) =>
    items.map((item) => {
      const targetUrl = item.targetUrl || item.target_url || "";
      return {
        body: item.body || defaultBody,
        headers: item.headers || defaultHeaders,
        params: item.params || defaultParams,
        targetUrl,
        ...item
      };
    })
    .map((item) => {
      const trimmedPath = item.path?.trim() || "/";
      if (!item.targetUrl && /^https?:\/\//i.test(trimmedPath)) {
        try {
          const parsed = new URL(trimmedPath);
          return {
            ...item,
            targetUrl: parsed.origin,
            path: `${parsed.pathname}${parsed.search}` || "/"
          };
        } catch (err) {
          return {
            ...item,
            targetUrl: trimmedPath,
            path: "/"
          };
        }
      }
      return item;
    });

  const seedIfEmpty = async () => {
    const existing = await loadEndpoints();
    if (existing.length) {
      return normalizeEndpoints(existing);
    }
    const seeded = {
      title: "GetSession",
      method: "POST",
      path: "/GetSession",
      description: "Session başlatma",
      targetUrl: "",
      body: defaultBody,
      headers: defaultHeaders,
      params: defaultParams
    };
    const { item: created } = await saveEndpoint(seeded);
    return created ? normalizeEndpoints([created]) : [];
  };

  const renderTable = (items, selectedId) => {
    const tables = [
      document.querySelector("#endpoint-table"),
      document.querySelector("#endpoint-table-inline")
    ].filter(Boolean);
    if (!tables.length) return;
    tables.forEach((table) => {
      table.innerHTML = "";
      if (!items.length) {
        table.innerHTML = `<div class="endpoint-row"><span class="desc">Henüz endpoint eklenmedi.</span></div>`;
        return;
      }
      items.forEach((item) => {
        const row = document.createElement("div");
        const itemId = Number(item.id);
        const selectable = Number.isInteger(itemId);
        const isActive = selectable && table.id !== "endpoint-table" && itemId === Number(selectedId);
        row.className = `endpoint-row selectable${isActive ? " active" : ""}`;
        if (selectable) {
          row.dataset.endpointId = String(itemId);
          row.draggable = true;
          row.classList.add("draggable");
        }
        row.innerHTML = `
          <span class="method ${(item.method || "GET").toLowerCase()}">${item.method || "GET"}</span>
          <div>
            <div class="path">${item.path}</div>
            <div class="desc">${item.title}${item.description ? " — " + item.description : ""}</div>
          </div>
          <div class="endpoint-row-actions">
            ${
              selectable
                ? `<button type="button" class="ghost small endpoint-edit" data-endpoint-id="${itemId}" draggable="false">Düzenle</button>`
                : ""
            }
          </div>
        `;
        table.appendChild(row);
      });
    });
  };

  const renderDetails = (item, editors) => {
    const title = document.querySelector("#endpoint-title");
    const path = document.querySelector("#endpoint-path");
    const method = document.querySelector("#endpoint-method");
    const body = document.querySelector("#endpoint-body");
    const headers = document.querySelector("#endpoint-headers");
    const params = document.querySelector("#endpoint-params");
    if (!title || !path || !method || !body || !headers) return;
    title.textContent = item.title;
    path.textContent = `${item.method} ${item.path}`;
    method.textContent = item.method;
    body.value = item.body || defaultBody;
    headers.value = item.headers || defaultHeaders;
    if (params) {
      params.value = item.params || defaultParams;
    }
    if (editors?.headers) editors.headers.render();
    if (editors?.params && params) editors.params.render();
  };

  const parseJsonSafe = (value) => {
    if (!value || !value.trim()) return {};
    try {
      return JSON.parse(value);
    } catch (err) {
      return {};
    }
  };

  const initRowEditor = ({ rowsContainer, addButton, textarea, copyJsonButton }) => {
    if (!rowsContainer || !textarea) return null;

    const addRow = (key = "", value = "", enabled = true) => {
      const row = document.createElement("div");
      row.className = "kv-row";
      row.innerHTML = `
        <input type="checkbox" class="kv-enabled" ${enabled ? "checked" : ""} />
        <input type="text" class="kv-key" placeholder="Key" value="${key}" />
        <input type="text" class="kv-value" placeholder="Value" value="${value}" />
        <button type="button" class="ghost small kv-remove">Sil</button>
      `;
      row.querySelector(".kv-remove")?.addEventListener("click", () => {
        row.remove();
        syncRows();
      });
      row.querySelectorAll("input").forEach((input) => {
        input.addEventListener("input", syncRows);
        input.addEventListener("change", syncRows);
      });
      rowsContainer.appendChild(row);
    };

    const syncRows = () => {
      const data = {};
      rowsContainer.querySelectorAll(".kv-row").forEach((row) => {
        const enabled = row.querySelector(".kv-enabled")?.checked;
        const key = row.querySelector(".kv-key")?.value?.trim();
        const value = row.querySelector(".kv-value")?.value ?? "";
        if (!enabled || !key) return;
        data[key] = value;
      });
      textarea.value = JSON.stringify(data, null, 2);
      textarea.dispatchEvent(new Event("input"));
    };

    const render = () => {
      rowsContainer.innerHTML = "";
      const data = parseJsonSafe(textarea.value);
      const entries = Object.entries(data || {});
      if (!entries.length) {
        addRow();
        return;
      }
      entries.forEach(([key, value]) => addRow(key, String(value ?? ""), true));
    };

    addButton?.addEventListener("click", () => {
      addRow();
    });

    copyJsonButton?.addEventListener("click", async () => {
      const text = textarea.value || "{}";
      if (navigator.clipboard?.writeText) {
        await navigator.clipboard.writeText(text);
      } else {
        window.prompt("Kopyala:", text);
      }
    });

    return { render, syncRows };
  };

  const renderTargets = (targets, selectedValue = "") => {
    const targetInput = document.querySelector("#target-url-input");
    if (!targetInput) return;
    const currentValue = String(targetInput.value || "").trim();
    const preferredValue = String(selectedValue || "").trim();
    const options = Array.from(
      new Set(
        (targets || [])
          .map((item) => (typeof item === "string" ? item : item?.url))
          .map((value) => String(value || "").trim())
          .filter((value) => value)
      )
    );
    targetInput.innerHTML = "";
    if (!options.length) {
      const emptyOption = document.createElement("option");
      emptyOption.value = "";
      emptyOption.textContent = "Kayıtlı hedef URL yok";
      targetInput.appendChild(emptyOption);
      targetInput.value = "";
      return;
    }

    options.forEach((url) => {
      const option = document.createElement("option");
      option.value = url;
      option.textContent = url;
      targetInput.appendChild(option);
    });

    let nextValue = preferredValue || currentValue || options[0];
    if (!options.includes(nextValue)) {
      nextValue = options[0];
    }
    targetInput.value = nextValue;
  };

  const normalizeEndpointAddress = ({ targetUrl, path }) => {
    const normalizedTargetUrl = (targetUrl || "").trim();
    let normalizedPath = (path || "").trim() || "/";

    if (/^https?:\/\//i.test(normalizedTargetUrl)) {
      try {
        const parsedTarget = new URL(normalizedTargetUrl);
        const basePath =
          parsedTarget.pathname && parsedTarget.pathname !== "/"
            ? parsedTarget.pathname.replace(/\/+$/, "")
            : "";

        if (basePath) {
          if (normalizedPath === "/") {
            normalizedPath = `${basePath}${parsedTarget.search || ""}` || "/";
          } else if (
            normalizedPath.startsWith("/") &&
            normalizedPath !== basePath &&
            !normalizedPath.startsWith(`${basePath}/`)
          ) {
            normalizedPath = `${basePath}${normalizedPath}`;
          }
        }

        return {
          targetUrl: parsedTarget.origin,
          path: normalizedPath || "/"
        };
      } catch (err) {
        // Ignore invalid target URL and keep the current value for validation.
      }
    }

    if (/^https?:\/\//i.test(normalizedPath)) {
      try {
        const parsed = new URL(normalizedPath);
        return {
          targetUrl: normalizedTargetUrl || parsed.origin,
          path: `${parsed.pathname}${parsed.search}` || "/"
        };
      } catch (err) {
        return {
          targetUrl: normalizedTargetUrl,
          path: normalizedPath
        };
      }
    }

    return {
      targetUrl: normalizedTargetUrl,
      path: normalizedPath
    };
  };

  const renderHistory = (items) => {
    const list = document.querySelector("#request-history");
    if (!list) return;
    list.innerHTML = "";
    if (!items.length) {
      list.innerHTML = `<div class="endpoint-row"><span class="desc">Henüz istek yok.</span></div>`;
      return;
    }
    items.forEach((item) => {
      const row = document.createElement("div");
      row.className = "history-item";
      row.dataset.requestId = item.id;
      const statusText = item.response_status ? `${item.response_status}` : "Hata";
      const createdAt = item.created_at
        ? new Date(item.created_at).toLocaleString("tr-TR")
        : "";
      row.innerHTML = `
        <span class="method ${item.method.toLowerCase()}">${item.method}</span>
        <div class="meta">
          <div class="title">${item.path}</div>
          <div class="sub">${createdAt} · ${item.duration_ms || 0} ms</div>
        </div>
        <span class="status">${statusText}</span>
      `;
      list.appendChild(row);
    });
  };

  const renderHistoryDetail = (item) => {
    const requestEl = document.querySelector("#history-request");
    const responseEl = document.querySelector("#history-response");
    const metaEl = document.querySelector("#history-meta");
    if (!requestEl || !responseEl || !metaEl) return;
    if (!item) {
      metaEl.textContent = "Seçim yok";
      requestEl.textContent = "{}";
      responseEl.textContent = "{}";
      return;
    }
    const createdAt = item.created_at
      ? new Date(item.created_at).toLocaleString("tr-TR")
      : "";
    metaEl.textContent = `${item.method} ${item.path} · ${createdAt}`;
    const requestPayload = {
      targetUrl: item.target_url,
      headers: item.headers ? JSON.parse(item.headers) : {},
      params: item.params ? JSON.parse(item.params) : {},
      body: item.body || ""
    };
    const responsePayload = {
      status: item.response_status,
      headers: item.response_headers ? JSON.parse(item.response_headers) : {},
      body: item.response_text || ""
    };
    requestEl.textContent = JSON.stringify(requestPayload, null, 2);
    responseEl.textContent = JSON.stringify(responsePayload, null, 2);
  };

  const initTabs = () => {
    document.querySelectorAll(".tab").forEach((tab) => {
      tab.addEventListener("click", () => {
        const target = tab.dataset.tab;
        document.querySelectorAll(".tab").forEach((t) => t.classList.remove("active"));
        tab.classList.add("active");
        document.querySelectorAll(".tab-panel").forEach((panel) => {
          panel.classList.toggle("active", panel.dataset.panel === target);
        });
      });
    });
  };

  const initSalesTabs = () => {
    const tabs = Array.from(document.querySelectorAll(".sales-tab"));
    const panels = Array.from(document.querySelectorAll(".sales-tab-panel"));
    if (tabs.length === 0 || panels.length === 0) return;

    tabs.forEach((tab) => {
      if (tab.dataset.bound === "1") return;
      tab.dataset.bound = "1";
      tab.addEventListener("click", () => {
        const target = tab.dataset.salesTab;
        tabs.forEach((item) => item.classList.toggle("active", item === tab));
        panels.forEach((panel) => {
          panel.classList.toggle("active", panel.dataset.salesPanel === target);
        });
      });
    });
  };

  const initSalesReportLoading = () => {
    const form = document.querySelector(".sales-filter-form");
    if (!form) return;
    if (form.dataset.loadingBound === "1") return;
    form.dataset.loadingBound = "1";

    const submitBtn = form.querySelector(".sales-filter-actions button[type='submit']");
    const loadingMessage = form.querySelector(".sales-loading-message");
    if (!submitBtn) return;

    form.classList.remove("is-loading");
    submitBtn.disabled = false;
    submitBtn.textContent = "Filtrele";
    if (loadingMessage) {
      loadingMessage.hidden = true;
    }

    form.addEventListener("submit", () => {
      submitBtn.disabled = true;
      submitBtn.textContent = "Yükleniyor...";
      form.classList.add("is-loading");
      if (loadingMessage) {
        loadingMessage.hidden = false;
      }
    });
  };

  const initSlackReportLoading = () => {
    const form = document.querySelector(".slack-filter-form");
    if (form) {
      if (form.dataset.loadingBound !== "1") {
        form.dataset.loadingBound = "1";
        const submitBtn = form.querySelector(".slack-filter-actions button[type='submit']");
        const loadingMessage = form.querySelector(".slack-loading-message");

        if (submitBtn) {
          form.classList.remove("is-loading");
          submitBtn.disabled = false;
          submitBtn.textContent = "Filtrele";
          if (loadingMessage) {
            loadingMessage.hidden = true;
          }

          form.addEventListener("submit", () => {
            submitBtn.disabled = true;
            submitBtn.textContent = "Yükleniyor...";
            form.classList.add("is-loading");
            if (loadingMessage) {
              loadingMessage.hidden = false;
            }
          });
        }
      }
    }

    const saveForm = document.querySelector(".slack-save-form");
    if (!saveForm || saveForm.dataset.loadingBound === "1") return;
    saveForm.dataset.loadingBound = "1";

    saveForm.addEventListener("submit", () => {
      const button = saveForm.querySelector("button[type='submit']");
      if (!button) return;
      button.disabled = true;
      button.textContent = "Kaydediliyor...";
    });
  };

  const initAllowedLinesLoading = () => {
    const form = document.querySelector(".allowed-lines-form");
    if (!form) return;
    if (form.dataset.loadingBound === "1") return;
    form.dataset.loadingBound = "1";

    const submitButtons = Array.from(form.querySelectorAll(".allowed-lines-actions button[type='submit']"));
    const loadingMessage = form.querySelector(".allowed-lines-loading-message");
    const companySelect = form.querySelector("#allowed-lines-company");
    const endpointInput = form.querySelector("#allowed-lines-endpoint-url");
    const endpointPreview = form.querySelector("[data-endpoint-preview]");
    const submitActionInput = form.querySelector("#allowed-lines-submit-action");
    const companySourceUrl = String(form.dataset.companySourceUrl || "").trim();
    if (!submitButtons.length) return;

    const syncEndpointPreview = () => {
      if (!endpointPreview || !endpointInput) return;
      endpointPreview.value = String(endpointInput.value || "").trim();
    };

    const replaceClusterInUrl = (urlValue, clusterValue) => {
      const url = String(urlValue || "").trim();
      const cluster = String(clusterValue || "").trim().toLowerCase();
      if (!url || !cluster) return url;
      if (/cluster\d+/i.test(url)) {
        return url.replace(/cluster\d+/i, cluster);
      }
      return url;
    };

    const extractClusterFromCompanyValue = (value) => {
      const raw = String(value || "").trim();
      if (!raw || !raw.includes("|||")) return "";
      const parts = raw.split("|||");
      if (parts.length < 3) return "";
      return String(parts[2] || "").trim().toLowerCase();
    };

    const applyCompanyClusterToEndpointUrl = () => {
      if (!companySelect || !endpointInput) return;
      const selectedOption = companySelect.options[companySelect.selectedIndex] || null;
      const selectedCluster =
        String(selectedOption?.dataset?.cluster || "").trim().toLowerCase() ||
        extractClusterFromCompanyValue(companySelect.value);
      if (!selectedCluster) {
        endpointInput.value = "";
        syncEndpointPreview();
        return;
      }

      const baseUrl = companySourceUrl || endpointInput.value || "";
      const nextUrl = replaceClusterInUrl(baseUrl, selectedCluster);
      if (nextUrl) {
        endpointInput.value = nextUrl;
      }
      syncEndpointPreview();
    };

    form.classList.remove("is-loading");
    submitButtons.forEach((button) => {
      button.disabled = false;
      button.textContent = String(button.dataset.defaultLabel || button.textContent || "").trim();
    });
    if (loadingMessage) {
      loadingMessage.hidden = true;
    }

    if (companySelect && endpointInput) {
      companySelect.addEventListener("change", applyCompanyClusterToEndpointUrl);
      if (!String(endpointInput.value || "").trim()) {
        applyCompanyClusterToEndpointUrl();
      } else {
        syncEndpointPreview();
      }
    } else {
      syncEndpointPreview();
    }

    submitButtons.forEach((button) => {
      button.addEventListener("click", () => {
        if (!submitActionInput) return;
        submitActionInput.value = String(button.dataset.submitAction || "authorized-lines").trim() || "authorized-lines";
      });
    });

    form.addEventListener("submit", (event) => {
      const activeSubmitter =
        event.submitter && submitButtons.includes(event.submitter) ? event.submitter : submitButtons[0];
      if (submitActionInput) {
        submitActionInput.value = String(
          activeSubmitter?.dataset?.submitAction || submitActionInput.value || "authorized-lines"
        ).trim() || "authorized-lines";
      }
      submitButtons.forEach((button) => {
        button.disabled = true;
        button.textContent = String(button.dataset.defaultLabel || button.textContent || "").trim();
      });
      if (activeSubmitter) {
        activeSubmitter.textContent = String(
          activeSubmitter.dataset.loadingLabel || activeSubmitter.dataset.defaultLabel || "Yükleniyor..."
        ).trim();
      }
      form.classList.add("is-loading");
      if (loadingMessage) {
        loadingMessage.hidden = false;
      }
    });
  };

  const initJourneyUpdateTableFilters = () => {
    const table = document.querySelector("[data-journey-update-table='1']");
    if (!table) return;
    if (table.dataset.filtersBound === "1") return;
    table.dataset.filtersBound = "1";

    const tbody = table.querySelector("tbody");
    const rows = Array.from(tbody?.querySelectorAll("tr") || []);
    const filters = Array.from(table.querySelectorAll("[data-journey-filter-key]"));
    const sortButtons = Array.from(table.querySelectorAll("[data-journey-sort-key]"));
    const countEl = document.querySelector("[data-journey-update-count='1']");
    const emptyEl = document.querySelector("[data-journey-update-empty='1']");
    const clearBtn = document.querySelector("[data-journey-clear-filters='1']");
    const totalCount = rows.length;
    const originalOrder = rows.slice();
    let sortState = {
      key: "",
      direction: "",
      type: "text"
    };

    if (!rows.length || !filters.length) return;

    const normalize = (value) => String(value || "").trim().toLocaleLowerCase("tr");
    const getCellText = (row, key) => String(row.querySelector(`[data-journey-cell='${key}']`)?.textContent || "").trim();
    const parseDateKey = (value) => {
      const text = String(value || "").trim();
      const match = text.match(/^(\d{4})-(\d{2})-(\d{2})(?:[T\s](\d{2}):(\d{2})(?::(\d{2}))?)?/);
      if (match) {
        return `${match[1]}${match[2]}${match[3]}${match[4] || "00"}${match[5] || "00"}${match[6] || "00"}`;
      }
      const parsed = new Date(text);
      if (Number.isNaN(parsed.getTime())) return "";
      const year = String(parsed.getFullYear()).padStart(4, "0");
      const month = String(parsed.getMonth() + 1).padStart(2, "0");
      const day = String(parsed.getDate()).padStart(2, "0");
      const hour = String(parsed.getHours()).padStart(2, "0");
      const minute = String(parsed.getMinutes()).padStart(2, "0");
      const second = String(parsed.getSeconds()).padStart(2, "0");
      return `${year}${month}${day}${hour}${minute}${second}`;
    };
    const parseNumberValue = (value) => {
      const numeric = Number.parseFloat(String(value || "").replace(",", "."));
      return Number.isFinite(numeric) ? numeric : Number.POSITIVE_INFINITY;
    };
    const parseDurationValue = (value) => {
      const match = String(value || "").trim().match(/^(\d{2}):(\d{2}):(\d{2})$/);
      if (!match) return Number.POSITIVE_INFINITY;
      return Number(match[1]) * 3600 + Number(match[2]) * 60 + Number(match[3]);
    };

    const updateCount = (visibleCount) => {
      if (!countEl) return;
      countEl.textContent =
        visibleCount === totalCount ? `${totalCount} satır` : `${visibleCount} / ${totalCount} satır`;
    };

    const compareRows = (rowA, rowB) => {
      if (!sortState.key || !sortState.direction) {
        return originalOrder.indexOf(rowA) - originalOrder.indexOf(rowB);
      }

      const valueA = getCellText(rowA, sortState.key);
      const valueB = getCellText(rowB, sortState.key);
      let comparison = 0;

      if (sortState.type === "date") {
        comparison = parseDateKey(valueA).localeCompare(parseDateKey(valueB), "tr");
      } else if (sortState.type === "number") {
        comparison = parseNumberValue(valueA) - parseNumberValue(valueB);
      } else if (sortState.type === "duration") {
        comparison = parseDurationValue(valueA) - parseDurationValue(valueB);
      } else {
        comparison = normalize(valueA).localeCompare(normalize(valueB), "tr");
      }

      if (comparison === 0) {
        comparison = originalOrder.indexOf(rowA) - originalOrder.indexOf(rowB);
      }
      return sortState.direction === "desc" ? comparison * -1 : comparison;
    };

    const updateSortButtons = () => {
      sortButtons.forEach((buttonEl) => {
        const key = String(buttonEl.getAttribute("data-journey-sort-key") || "").trim();
        const isActive = key === sortState.key && Boolean(sortState.direction);
        buttonEl.dataset.sortDirection = isActive ? sortState.direction : "";
        buttonEl.textContent = !isActive ? "↕" : sortState.direction === "asc" ? "↑" : "↓";
      });
    };

    const applyFilters = () => {
      const sortedRows = rows.slice().sort(compareRows);
      const fragment = document.createDocumentFragment();
      let visibleCount = 0;

      sortedRows.forEach((row) => {
        const matches = filters.every((filterEl) => {
          const key = String(filterEl.getAttribute("data-journey-filter-key") || "").trim();
          if (!key) return true;

          const rawFilterValue = String(filterEl.value || "").trim();
          const filterValue = normalize(rawFilterValue);
          if (!filterValue) return true;

          const rawCellText = getCellText(row, key);
          const cellText = normalize(rawCellText);
          const filterType = String(filterEl.getAttribute("data-journey-filter-type") || "").trim();
          const matchMode = String(filterEl.getAttribute("data-journey-filter-match") || "contains").trim();
          if (filterType === "date") {
            return rawCellText.startsWith(rawFilterValue);
          }
          if (matchMode === "exact") {
            return cellText === filterValue;
          }
          return cellText.includes(filterValue);
        });

        row.hidden = !matches;
        if (matches) visibleCount += 1;
        fragment.appendChild(row);
      });

      tbody?.appendChild(fragment);
      updateCount(visibleCount);
      updateSortButtons();
      if (emptyEl) {
        emptyEl.hidden = visibleCount > 0;
      }
    };

    filters.forEach((filterEl) => {
      const eventName = filterEl.tagName === "SELECT" ? "change" : "input";
      filterEl.addEventListener(eventName, applyFilters);
    });

    clearBtn?.addEventListener("click", () => {
      filters.forEach((filterEl) => {
        filterEl.value = "";
      });
      applyFilters();
      const firstTextInput = filters.find((filterEl) => filterEl.tagName === "INPUT");
      firstTextInput?.focus();
    });

    sortButtons.forEach((buttonEl) => {
      buttonEl.addEventListener("click", () => {
        const key = String(buttonEl.getAttribute("data-journey-sort-key") || "").trim();
        const type = String(buttonEl.getAttribute("data-journey-sort-type") || "text").trim();
        if (!key) return;

        if (sortState.key !== key) {
          sortState = { key, direction: "asc", type };
        } else if (sortState.direction === "asc") {
          sortState = { key, direction: "desc", type };
        } else if (sortState.direction === "desc") {
          sortState = { key: "", direction: "", type: "text" };
        } else {
          sortState = { key, direction: "asc", type };
        }
        applyFilters();
      });
    });

    applyFilters();
  };

  const initJourneyUpdateEditorForm = () => {
    const form = document.querySelector("[data-journey-update-editor-form='1']");
    if (!form) return;
    if (form.dataset.bound === "1") return;
    form.dataset.bound = "1";

    const fieldEls = Array.from(form.querySelectorAll("[data-journey-update-editor-key]"));
    const rowIdsInput = form.querySelector("[data-journey-update-row-ids='1']");
    const detailStateInput = form.querySelector("[data-journey-update-detail-state='1']");
    const tableRowsStateInput = form.querySelector("input[name='tableRowsState']");
    const tableColumnsStateInput = form.querySelector("input[name='tableColumnsState']");
    const requestUrlEl = form.querySelector("[data-journey-update-request-url]");
    const requestHeadersEl = form.querySelector("[data-journey-update-request-headers]");
    const requestBodyEl = form.querySelector("[data-journey-update-request-body]");
    const countEl = document.querySelector("[data-journey-update-preview-count='1']");
    const submitButton = form.querySelector("button[type='submit']");
    const loadingMessage = form.querySelector(".journey-update-editor-loading");
    const endpointUrl = String(form.dataset.journeyUpdateEndpointUrl || "").trim();

    const normalizeTokenName = (value) =>
      String(value || "")
        .toLowerCase()
        .replace(/[^a-z0-9]/g, "");

    const buildDynamicColumnKey = (fieldName) => {
      const normalized = normalizeTokenName(fieldName);
      return normalized ? `parameter_${normalized}` : "";
    };

    const buildFieldAliases = (fieldName, extraAliases = []) =>
      Array.from(new Set([fieldName, buildDynamicColumnKey(fieldName), ...(Array.isArray(extraAliases) ? extraAliases : [])]));

    const requestFieldSpecs = [
      { field: "description", aliases: buildFieldAliases("description"), valueType: "string" },
      { field: "bus-id", aliases: buildFieldAliases("bus-id"), valueType: "nullable-number" },
      { field: "original-bus-type-id", aliases: buildFieldAliases("original-bus-type-id"), valueType: "nullable-number" },
      { field: "destination-display-name", aliases: buildFieldAliases("destination-display-name"), valueType: "string" },
      { field: "is-active", aliases: buildFieldAliases("is-active"), valueType: "boolean" },
      { field: "is-additional", aliases: buildFieldAliases("is-additional"), valueType: "boolean" },
      { field: "duration", aliases: buildFieldAliases("duration"), valueType: "nullable-string" },
      { field: "type", aliases: buildFieldAliases("type", ["journeyType", "journey-type"]), valueType: "boolean" },
      { field: "distance", aliases: buildFieldAliases("distance"), valueType: "nullable-number" },
      { field: "code", aliases: buildFieldAliases("code", ["journeyCode", "journey-code"]), valueType: "string" },
      { field: "departure-time", aliases: buildFieldAliases("departure-time", ["departureTime"]), valueType: "string" },
      { field: "id", aliases: buildFieldAliases("id"), valueType: "journey-id" },
      { field: "route-id", aliases: buildFieldAliases("route-id"), valueType: "nullable-number" },
      { field: "name", aliases: buildFieldAliases("name"), valueType: "string" },
      {
        field: "extend-journey-activation",
        aliases: buildFieldAliases("extend-journey-activation"),
        valueType: "boolean"
      }
    ];

    const parseJourneyId = (value) => {
      const text = String(value || "").trim();
      if (!text) return "";
      if (/^-?\d+$/.test(text)) {
        const parsed = Number.parseInt(text, 10);
        if (Number.isSafeInteger(parsed)) return parsed;
      }
      return text;
    };

    const getJourneyIds = () =>
      String(rowIdsInput?.value || "")
        .split(",")
        .map((item) => String(item || "").trim())
        .filter(Boolean);

    const decodeBase64Utf8 = (value) => {
      const raw = String(value || "").trim();
      if (!raw) return "";
      try {
        const binary = window.atob(raw);
        const bytes = Uint8Array.from(binary, (char) => char.charCodeAt(0));
        if (typeof TextDecoder !== "undefined") {
          return new TextDecoder("utf-8").decode(bytes);
        }
        return Array.from(bytes, (byte) => String.fromCharCode(byte)).join("");
      } catch (err) {
        return "";
      }
    };

    const parseBase64Json = (value) => {
      const text = decodeBase64Utf8(value);
      if (!text) return null;
      try {
        return JSON.parse(text);
      } catch (err) {
        return null;
      }
    };

    const getTableRows = () => {
      const parsed = parseBase64Json(tableRowsStateInput?.value || "");
      if (Array.isArray(parsed)) {
        return parsed.filter((item) => item && typeof item === "object" && !Array.isArray(item));
      }
      return getJourneyIds().map((id) => ({ id }));
    };

    const getTableColumns = () => {
      const parsed = parseBase64Json(tableColumnsStateInput?.value || "");
      return Array.isArray(parsed) ? parsed.filter((item) => item && typeof item === "object" && !Array.isArray(item)) : [];
    };

    const getDetailState = () => {
      const parsed = parseBase64Json(detailStateInput?.value || "");
      return parsed && typeof parsed === "object" && !Array.isArray(parsed) ? parsed : {};
    };

    const cloneJson = (value) => {
      if (value === undefined) return undefined;
      try {
        return JSON.parse(JSON.stringify(value));
      } catch (err) {
        return undefined;
      }
    };

    const normalizeCellText = (value) => {
      const text = String(value ?? "").trim();
      if (!text || text === "-" || /^(null|undefined)$/i.test(text)) return "";
      return text;
    };

    const hasRowValue = (value) => {
      if (typeof value === "boolean") return true;
      if (typeof value === "number") return Number.isFinite(value);
      return Boolean(normalizeCellText(value));
    };

    const parseBooleanValue = (value) => {
      if (typeof value === "boolean") return value;
      if (typeof value === "number") {
        if (value === 1) return true;
        if (value === 0) return false;
        return null;
      }
      const normalized = normalizeTokenName(value);
      if (!normalized) return null;
      if (["true", "1", "yes", "evet", "t", "y", "on"].includes(normalized)) return true;
      if (["false", "0", "no", "hayir", "f", "n", "off"].includes(normalized)) return false;
      return null;
    };

    const parseNullableNumberValue = (value) => {
      const text = normalizeCellText(value);
      if (!text) return null;
      if (/^-?\d+$/.test(text)) {
        const parsed = Number.parseInt(text, 10);
        if (Number.isSafeInteger(parsed)) return parsed;
      }
      const parsedNumber = Number(text.replace(",", "."));
      return Number.isFinite(parsedNumber) ? parsedNumber : null;
    };

    const parseFieldValueByType = (value, valueType = "string") => {
      switch (String(valueType || "").trim()) {
        case "journey-id":
          return parseJourneyId(value);
        case "nullable-number":
          return parseNullableNumberValue(value);
        case "nullable-string": {
          const text = normalizeCellText(value);
          return text || null;
        }
        case "boolean": {
          const parsedBoolean = parseBooleanValue(value);
          return parsedBoolean === null ? undefined : parsedBoolean;
        }
        case "string":
        default: {
          const text = normalizeCellText(value);
          return text || "";
        }
      }
    };

    const buildRequestDate = () => {
      const fromDataset = String(form.dataset.journeyUpdateRequestDate || "").trim();
      if (fromDataset) return fromDataset;
      const now = new Date();
      const year = String(now.getFullYear()).padStart(4, "0");
      const month = String(now.getMonth() + 1).padStart(2, "0");
      const day = String(now.getDate()).padStart(2, "0");
      const hour = String(now.getHours()).padStart(2, "0");
      const minute = String(now.getMinutes()).padStart(2, "0");
      const second = String(now.getSeconds()).padStart(2, "0");
      return `${year}-${month}-${day} ${hour}:${minute}:${second}`;
    };

    const buildParameters = () =>
      fieldEls
        .map((fieldEl) => {
          const rawValue = String(fieldEl.value || "").trim();
          if (!rawValue) return null;
          const parameterType = String(fieldEl.getAttribute("data-journey-update-param-type") || "").trim();
          const valueType = String(fieldEl.getAttribute("data-journey-update-value-type") || "text").trim();
          if (!parameterType) return null;
          if (valueType === "boolean") {
            if (rawValue === "true") {
              return { type: parameterType, value: true };
            }
            if (rawValue === "false") {
              return { type: parameterType, value: false };
            }
            return null;
          }
          return {
            type: parameterType,
            value: rawValue
          };
        })
        .filter(Boolean);

    const normalizeParameterEntry = (item) => {
      if (!item || typeof item !== "object" || Array.isArray(item)) return null;
      const type = String(item.type || "").trim();
      if (!type) return null;
      if (typeof item.value === "boolean") return { type, value: item.value };
      if (typeof item.value === "number") {
        return Number.isFinite(item.value) ? { type, value: item.value } : null;
      }
      const textValue = String(item.value ?? "").trim();
      return textValue ? { type, value: textValue } : null;
    };

    const mergeParameters = (existingParameters, overrideParameters) => {
      const merged = [];
      const indexByType = new Map();

      (Array.isArray(existingParameters) ? existingParameters : [])
        .map(normalizeParameterEntry)
        .filter(Boolean)
        .forEach((item) => {
          const normalizedType = normalizeTokenName(item.type);
          if (!normalizedType || indexByType.has(normalizedType)) return;
          indexByType.set(normalizedType, merged.length);
          merged.push(item);
        });

      (Array.isArray(overrideParameters) ? overrideParameters : [])
        .map(normalizeParameterEntry)
        .filter(Boolean)
        .forEach((item) => {
          const normalizedType = normalizeTokenName(item.type);
          if (!normalizedType) return;
          if (indexByType.has(normalizedType)) {
            merged[indexByType.get(normalizedType)] = item;
            return;
          }
          indexByType.set(normalizedType, merged.length);
          merged.push(item);
        });

      return merged;
    };

    const buildRowLookup = (row, columns) => {
      const lookup = new Map();
      const safeRow = row && typeof row === "object" && !Array.isArray(row) ? row : {};

      Object.entries(safeRow).forEach(([key, value]) => {
        const normalizedKey = normalizeTokenName(key);
        if (!normalizedKey || lookup.has(normalizedKey)) return;
        lookup.set(normalizedKey, value);
      });

      (Array.isArray(columns) ? columns : []).forEach((column) => {
        const columnKey = String(column?.key || "").trim();
        if (!columnKey) return;
        const value = safeRow[columnKey];
        const normalizedColumnKey = normalizeTokenName(columnKey);
        const normalizedColumnLabel = normalizeTokenName(column?.label);
        if (normalizedColumnKey && !lookup.has(normalizedColumnKey)) {
          lookup.set(normalizedColumnKey, value);
        }
        if (normalizedColumnLabel && !lookup.has(normalizedColumnLabel)) {
          lookup.set(normalizedColumnLabel, value);
        }
      });

      return lookup;
    };

    const readLookupValue = (lookup, aliases) => {
      const aliasList = Array.isArray(aliases) ? aliases : [aliases];
      for (const alias of aliasList) {
        const normalizedAlias = normalizeTokenName(alias);
        if (!normalizedAlias || !lookup.has(normalizedAlias)) continue;
        return lookup.get(normalizedAlias);
      }
      return undefined;
    };

    const buildRequestData = (row, columns, detailState, overrideParameters) => {
      const journeyId = String(row?.id || "").trim();
      const safeDetailState = detailState && typeof detailState === "object" && !Array.isArray(detailState) ? detailState : {};
      const baseDetailData = cloneJson(safeDetailState[journeyId]);
      const data = baseDetailData && typeof baseDetailData === "object" && !Array.isArray(baseDetailData) ? baseDetailData : {};
      const lookup = buildRowLookup(row, columns);

      requestFieldSpecs.forEach((spec) => {
        const rawValue = readLookupValue(lookup, spec.aliases);
        if (!hasRowValue(rawValue)) return;
        const parsedValue = parseFieldValueByType(rawValue, spec.valueType);
        if (parsedValue !== undefined) {
          data[spec.field] = parsedValue;
        }
      });

      data.id = parseJourneyId(data.id || journeyId);
      data.parameters = mergeParameters(data.parameters, overrideParameters);
      if (!Array.isArray(data.staffs)) {
        data.staffs = [];
      }

      return data;
    };

    const buildRequestBody = (row, columns, detailState, parameters, requestDate) => ({
      data: {
        ...buildRequestData(row, columns, detailState, parameters)
      },
      "device-session": {
        "session-id": "{{sessionId}}",
        "device-id": "{{deviceId}}"
      },
      token: "{{token}}",
      date: requestDate,
      language: "tr-TR"
    });

    const syncPreview = () => {
      const tableRows = getTableRows();
      const tableColumns = getTableColumns();
      const detailState = getDetailState();
      const parameters = buildParameters();
      const requestDate = buildRequestDate();
      const sampleBodies = tableRows
        .slice(0, 3)
        .map((row) => buildRequestBody(row, tableColumns, detailState, parameters, requestDate));
      const previewPayload = {
        requestUrl: endpointUrl,
        requestCount: tableRows.length,
        parameterTypes: parameters.map((item) => item.type),
        requests: sampleBodies
      };
      if (tableRows.length > sampleBodies.length) {
        previewPayload.moreRequests = tableRows.length - sampleBodies.length;
      }

      if (requestUrlEl) {
        requestUrlEl.textContent = endpointUrl || "-";
      }
      if (requestHeadersEl) {
        requestHeadersEl.textContent = JSON.stringify(
          {
            "Content-Type": "application/json",
            Authorization: "Basic MTIzNDU2MHg2NTUwR21STG5QYXJ5bnVt"
          },
          null,
          2
        );
      }
      if (requestBodyEl) {
        requestBodyEl.textContent = JSON.stringify(previewPayload, null, 2);
      }
      if (countEl) {
        countEl.textContent = `${tableRows.length} istek`;
      }
    };

    fieldEls.forEach((fieldEl) => {
      const eventName = fieldEl.tagName === "SELECT" ? "change" : "input";
      fieldEl.addEventListener(eventName, syncPreview);
    });

    form.addEventListener("submit", () => {
      form.classList.add("is-loading");
      if (submitButton) {
        submitButton.disabled = true;
        submitButton.textContent = String(
          submitButton.dataset.loadingLabel || submitButton.dataset.defaultLabel || "Gönderiliyor..."
        ).trim();
      }
      if (loadingMessage) {
        loadingMessage.hidden = false;
      }
    });

    syncPreview();
  };

  const initJourneyUpdateFloatingScrollbar = () => {
    if (typeof window.__journeyUpdateFloatingScrollbarCleanup === "function") {
      window.__journeyUpdateFloatingScrollbarCleanup();
      window.__journeyUpdateFloatingScrollbarCleanup = null;
    }

    const card = document.querySelector("[data-journey-update-card='1']");
    const source = document.querySelector("[data-journey-table-scroll='1']");
    const floating = document.querySelector("[data-journey-floating-scroll='1']");
    const track = document.querySelector("[data-journey-floating-scroll-track='1']");
    if (!card || !source || !floating || !track) return;

    const listenerCleanups = [];
    let resizeObserver = null;
    let animationFrameId = 0;
    let syncingFromSource = false;
    let syncingFromFloating = false;

    const addListener = (target, eventName, handler, options) => {
      if (!target) return;
      target.addEventListener(eventName, handler, options);
      listenerCleanups.push(() => target.removeEventListener(eventName, handler, options));
    };

    const syncTrackWidth = () => {
      track.style.width = `${Math.max(source.scrollWidth, source.clientWidth)}px`;
    };

    const hideFloating = () => {
      floating.hidden = true;
    };

    const updateFloating = () => {
      animationFrameId = 0;
      syncTrackWidth();

      const sourceRect = source.getBoundingClientRect();
      const viewportWidth = window.innerWidth || document.documentElement.clientWidth || 0;
      const viewportHeight = window.innerHeight || document.documentElement.clientHeight || 0;
      const gap = 12;
      const barHeight = Math.max(floating.offsetHeight || 0, 16);
      const hasOverflow = source.scrollWidth > source.clientWidth + 1;
      const width = Math.min(sourceRect.width, viewportWidth - gap * 2);
      const minTop = Math.max(sourceRect.top, gap);
      const maxTop = Math.min(sourceRect.bottom - barHeight, viewportHeight - gap - barHeight);
      if (!hasOverflow || sourceRect.width <= 0 || width <= 0 || maxTop < minTop) {
        hideFloating();
        return;
      }

      const left = Math.min(Math.max(sourceRect.left, gap), viewportWidth - gap - width);
      floating.style.left = `${left}px`;
      floating.style.top = `${maxTop}px`;
      floating.style.width = `${width}px`;
      floating.hidden = false;

      if (Math.abs(floating.scrollLeft - source.scrollLeft) > 1) {
        syncingFromSource = true;
        floating.scrollLeft = source.scrollLeft;
        syncingFromSource = false;
      }
    };

    const scheduleUpdate = () => {
      if (animationFrameId) return;
      animationFrameId = window.requestAnimationFrame(updateFloating);
    };

    const handleSourceScroll = () => {
      if (syncingFromFloating) return;
      syncingFromSource = true;
      floating.scrollLeft = source.scrollLeft;
      syncingFromSource = false;
      scheduleUpdate();
    };

    const handleFloatingScroll = () => {
      if (syncingFromSource) return;
      syncingFromFloating = true;
      source.scrollLeft = floating.scrollLeft;
      syncingFromFloating = false;
    };

    addListener(source, "scroll", handleSourceScroll, { passive: true });
    addListener(floating, "scroll", handleFloatingScroll, { passive: true });
    addListener(window, "scroll", scheduleUpdate, { passive: true });
    addListener(window, "resize", scheduleUpdate, { passive: true });

    if (typeof ResizeObserver === "function") {
      resizeObserver = new ResizeObserver(() => {
        scheduleUpdate();
      });
      resizeObserver.observe(card);
      resizeObserver.observe(source);
      resizeObserver.observe(track);
    }

    window.__journeyUpdateFloatingScrollbarCleanup = () => {
      if (animationFrameId) {
        window.cancelAnimationFrame(animationFrameId);
        animationFrameId = 0;
      }
      resizeObserver?.disconnect();
      listenerCleanups.forEach((cleanup) => cleanup());
      floating.hidden = true;
    };

    scheduleUpdate();
  };

  const initJourneySearchForm = () => {
    const form = document.querySelector("[data-journey-search-form='1']");
    if (!form) return;
    if (form.dataset.bound === "1") return;
    form.dataset.bound = "1";

    const serviceButtons = Array.from(form.querySelectorAll("[data-journey-search-service-button]"));
    const servicePanels = Array.from(form.querySelectorAll("[data-journey-search-service-panel]"));
    const companySelect = form.querySelector("#journey-search-company");
    const originSelect = form.querySelector("#journey-search-origin");
    const destinationSelect = form.querySelector("#journey-search-destination");
    const showIdCheckbox = form.querySelector("#journey-search-show-id");
    const journeysCompanySelect = form.querySelector("#journey-search-journeys-company");
    const journeysOriginSelect = form.querySelector("#journey-search-journeys-origin");
    const journeysDestinationSelect = form.querySelector("#journey-search-journeys-destination");
    const journeysDateInput = form.querySelector("#journey-search-journeys-date");
    const requestBodyEl = form.querySelector("[data-journey-search-request-body='getstation']");
    const responseBodyEl = form.querySelector("[data-journey-search-response-body='getstation']");
    const httpBadgeEl = form.querySelector("[data-journey-search-http='getstation']");
    const urlLineEl = form.querySelector("[data-journey-search-url='getstation']");
    const journeysRequestBodyEl = form.querySelector("[data-journey-search-request-body='getjourneys']");
    const journeysResponseBodyEl = form.querySelector("[data-journey-search-response-body='getjourneys']");
    const journeysHttpBadgeEl = form.querySelector("[data-journey-search-http='getjourneys']");
    const journeysUrlLineEl = form.querySelector("[data-journey-search-url='getjourneys']");
    const journeysResultsPanelEl = form.querySelector("[data-journey-search-results-panel='getjourneys']");
    const journeysResultsEl = form.querySelector("[data-journey-search-results='getjourneys']");
    const statusEl = form.querySelector("[data-journey-search-status='1']");
    if (
      serviceButtons.length === 0 ||
      !companySelect ||
      !originSelect ||
      !destinationSelect ||
      !showIdCheckbox ||
      !journeysCompanySelect ||
      !journeysOriginSelect ||
      !journeysDestinationSelect ||
      !journeysDateInput ||
      !journeysRequestBodyEl ||
      !journeysResponseBodyEl ||
      !journeysHttpBadgeEl ||
      !journeysUrlLineEl ||
      !journeysResultsPanelEl ||
      !journeysResultsEl ||
      !requestBodyEl ||
      !responseBodyEl ||
      !httpBadgeEl ||
      !urlLineEl ||
      !statusEl
    ) {
      return;
    }

    const formatDateInputValue = (date) => {
      if (!(date instanceof Date) || Number.isNaN(date.getTime())) return "";
      const year = String(date.getFullYear());
      const month = String(date.getMonth() + 1).padStart(2, "0");
      const day = String(date.getDate()).padStart(2, "0");
      return `${year}-${month}-${day}`;
    };

    if (!String(journeysDateInput.value || "").trim()) {
      journeysDateInput.value = formatDateInputValue(new Date());
    }

    let selectedOriginValue = String(originSelect.dataset.selectedValue || "").trim();
    let selectedDestinationValue = String(destinationSelect.dataset.selectedValue || "").trim();
    let selectedJourneyOriginId = "";
    let selectedJourneyDestinationId = "";
    let selectedJourneysDate = String(journeysDateInput.value || "").trim();
    let requestSequence = 0;
    let journeysRequestSequence = 0;
    let loadedStationItems = [];
    let activeServiceKey = "";
    const stationRequestBodyTemplate = String(requestBodyEl.textContent || "{}").trim() || "{}";
    const ioTargets = {
      getstation: {
        requestBodyEl,
        responseBodyEl,
        httpBadgeEl,
        urlLineEl
      },
      getjourneys: {
        requestBodyEl: journeysRequestBodyEl,
        responseBodyEl: journeysResponseBodyEl,
        httpBadgeEl: journeysHttpBadgeEl,
        urlLineEl: journeysUrlLineEl
      }
    };

    const setStatus = (message, kind = "muted") => {
      statusEl.textContent = String(message || "").trim();
      if (kind === "error") {
        statusEl.className = "journey-search-status alert inline-alert";
        return;
      }
      if (kind === "success") {
        statusEl.className = "journey-search-status inline-success";
        return;
      }
      statusEl.className = "journey-search-status muted";
    };

    const setIoState = (serviceKey, { requestBody, responseBody = "Henüz response yok.", requestUrl = "", status = null } = {}) => {
      const target = ioTargets[serviceKey];
      if (!target) return;
      const fallbackBody = serviceKey === "getstation" ? stationRequestBodyTemplate : "";
      const normalizedRequestBody = String(requestBody ?? fallbackBody).trim() || fallbackBody || "";
      const normalizedResponseBody = String(responseBody || "").trim() || "Henüz response yok.";
      const parsedStatus = Number.parseInt(String(status ?? "").trim(), 10);
      target.requestBodyEl.textContent = normalizedRequestBody;
      target.responseBodyEl.textContent = normalizedResponseBody;
      if (Number.isFinite(parsedStatus) && parsedStatus > 0) {
        target.httpBadgeEl.textContent = `HTTP ${parsedStatus}`;
        target.httpBadgeEl.className = "pill";
      } else {
        target.httpBadgeEl.textContent = "HTTP -";
        target.httpBadgeEl.className = "pill muted";
      }
      target.urlLineEl.textContent = `URL: ${String(requestUrl || "").trim() || "-"}`;
    };

    const activateService = (serviceKey) => {
      activeServiceKey = String(serviceKey || "").trim();
      serviceButtons.forEach((button) => {
        const isActive = String(button.dataset.journeySearchServiceButton || "").trim() === activeServiceKey;
        button.classList.toggle("active", isActive);
        button.setAttribute("aria-pressed", isActive ? "true" : "false");
      });
      servicePanels.forEach((panel) => {
        panel.hidden = String(panel.dataset.journeySearchServicePanel || "").trim() !== activeServiceKey;
      });
      journeysResultsPanelEl.hidden =
        activeServiceKey !== "getjourneys" || journeysResultsEl.hidden || journeysResultsEl.childElementCount === 0;
      const applyServiceHint = () => {
        if (activeServiceKey === "getjourneys") {
          if (!companySelect.value) {
            setStatus("GetJourneys akışı için önce firma seçin.", "error");
            return false;
          }
          if (!selectedJourneyOriginId || !selectedJourneyDestinationId) {
            setStatus("Kalkış ve varış istasyonlarını seçin.", "muted");
            return false;
          }
          if (!selectedJourneysDate) {
            setStatus("GetJourneys için tarih seçin.", "muted");
            return false;
          }
          setStatus("GetJourneys isteği hazırlanıyor...", "muted");
          return true;
        }
        if (!companySelect.value) {
          setStatus("Firma seçince kalkış ve varış listesi yüklenecek.", "muted");
          return false;
        }
        if (loadedStationItems.length === 0) {
          setStatus("İstasyonlar yükleniyor...", "muted");
          return false;
        }
        setStatus(`${loadedStationItems.length} istasyon yüklendi.`, "success");
        return false;
      };
      if (applyServiceHint()) {
        maybeTriggerGetJourneysRequest();
      }
    };

    const getStationSelectedId = (selectEl) => {
      const selectedOption = selectEl.selectedOptions?.[0];
      if (!selectedOption) return String(selectEl.value || "").trim();
      return String(selectedOption.dataset?.stationId || selectEl.value || "").trim();
    };

    const buildJourneysSelectLabel = (item) => {
      const id = String(item?.id || item?.value || "").trim();
      const name = String(item?.name || item?.label || "").trim();
      if (id && name) {
        return `${name} - ${id}`;
      }
      return name || id;
    };

    const fillJourneysSelect = (selectEl, items, placeholder, selectedId = "") => {
      const normalizedItems = Array.isArray(items) ? items : [];
      const targetValue = String(selectedId || "").trim();
      const fragment = document.createDocumentFragment();
      const placeholderOption = document.createElement("option");
      placeholderOption.value = "";
      placeholderOption.textContent = placeholder || "Seçiniz";
      fragment.appendChild(placeholderOption);

      let hasMatch = false;
      normalizedItems.forEach((item) => {
        const value = String(item?.id || item?.value || "").trim();
        const label = buildJourneysSelectLabel(item);
        if (!value || !label) return;

        const option = document.createElement("option");
        option.value = value;
        option.textContent = label;
        option.dataset.stationId = String(item?.id || "").trim();
        if (targetValue && targetValue === value) {
          option.selected = true;
          hasMatch = true;
        }
        fragment.appendChild(option);
      });

      placeholderOption.selected = !hasMatch;
      selectEl.innerHTML = "";
      selectEl.appendChild(fragment);
    };

    const renderJourneysStationSelects = () => {
      if (!Array.isArray(loadedStationItems) || loadedStationItems.length === 0) {
        fillJourneysSelect(journeysOriginSelect, [], "Önce firma seçin", "");
        fillJourneysSelect(journeysDestinationSelect, [], "Önce firma seçin", "");
        return;
      }
      fillJourneysSelect(journeysOriginSelect, loadedStationItems, "Kalkış seçiniz", selectedJourneyOriginId);
      fillJourneysSelect(
        journeysDestinationSelect,
        loadedStationItems,
        "Varış seçiniz",
        selectedJourneyDestinationId
      );
    };

    const buildJourneysDateRange = (dateValue) => {
      const trimmed = String(dateValue || "").trim();
      if (!/^\d{4}-\d{2}-\d{2}$/.test(trimmed)) {
        return null;
      }
      return {
        from: `${trimmed}T00:00:00.000Z`,
        to: `${trimmed}T23:59:59.000Z`
      };
    };

    const parseJsonSafe = (value) => {
      try {
        return JSON.parse(String(value || ""));
      } catch (err) {
        return null;
      }
    };

    const clearJourneysResults = () => {
      journeysResultsEl.innerHTML = "";
      journeysResultsEl.hidden = true;
      journeysResultsPanelEl.hidden = true;
    };

    const getJourneyRouteStop = (journey, stationId, fallbackIndex) => {
      const routeItems = Array.isArray(journey?.route) ? journey.route : [];
      const targetId = String(stationId || "").trim();
      if (targetId) {
        const matchedStop = routeItems.find((item) => {
          const itemId = String(item?.id || "").trim();
          const originalOriginId = String(item?.["original-origin-id"] || "").trim();
          return itemId === targetId || originalOriginId === targetId;
        });
        if (matchedStop) {
          return matchedStop;
        }
      }
      if (Number.isInteger(fallbackIndex) && fallbackIndex >= 0 && fallbackIndex < routeItems.length) {
        return routeItems[fallbackIndex];
      }
      return null;
    };

    const getJourneyHourValue = (timeValue) => {
      const trimmed = String(timeValue || "").trim();
      const dateMatch = trimmed.match(/T(\d{2}:\d{2})/);
      if (dateMatch?.[1]) {
        return dateMatch[1];
      }
      const plainMatch = trimmed.match(/^(\d{2}:\d{2})/);
      return plainMatch?.[1] || "-";
    };

    const getJourneyPriceValue = (amount, currency) => {
      if (amount === null || amount === undefined || amount === "") {
        return "-";
      }
      const parsedAmount = Number(amount);
      const normalizedCurrency = String(currency || "").trim();
      if (Number.isFinite(parsedAmount)) {
        const renderedAmount = parsedAmount.toLocaleString("tr-TR");
        return normalizedCurrency ? `${renderedAmount} ${normalizedCurrency}` : renderedAmount;
      }
      const fallbackAmount = String(amount).trim();
      return normalizedCurrency ? `${fallbackAmount} ${normalizedCurrency}` : fallbackAmount || "-";
    };

    const createJourneyResultCell = (label, value) => {
      const cell = document.createElement("div");
      cell.className = "journey-search-result-cell";

      const labelEl = document.createElement("span");
      labelEl.className = "journey-search-result-label";
      labelEl.textContent = label;

      const valueEl = document.createElement("span");
      valueEl.className = "journey-search-result-value";
      valueEl.textContent = String(value || "").trim() || "-";

      cell.appendChild(labelEl);
      cell.appendChild(valueEl);
      return cell;
    };

    const renderJourneysResults = (responseBodyText, { originId = "", destinationId = "" } = {}) => {
      const parsedResponse = parseJsonSafe(responseBodyText);
      if (!Array.isArray(parsedResponse?.data)) {
        clearJourneysResults();
        return;
      }

      const journeyItems = parsedResponse.data;

      journeysResultsEl.innerHTML = "";
      journeysResultsEl.hidden = false;
      journeysResultsPanelEl.hidden = activeServiceKey !== "getjourneys";

      if (!journeyItems.length) {
        const emptyEl = document.createElement("div");
        emptyEl.className = "journey-search-result-empty";
        emptyEl.textContent = "Response içinde listelenecek sefer bulunamadı.";
        journeysResultsEl.appendChild(emptyEl);
        return;
      }

      journeyItems.forEach((journey) => {
        const routeItems = Array.isArray(journey?.route) ? journey.route : [];
        const departureStop = getJourneyRouteStop(journey, originId, 0);
        const arrivalStop = getJourneyRouteStop(
          journey,
          destinationId,
          routeItems.length > 0 ? routeItems.length - 1 : -1
        );
        const rowEl = document.createElement("div");
        rowEl.className = "journey-search-result-row";

        rowEl.appendChild(createJourneyResultCell("Saat", getJourneyHourValue(departureStop?.time)));
        rowEl.appendChild(createJourneyResultCell("Kalkış", departureStop?.name));
        rowEl.appendChild(createJourneyResultCell("Varış", arrivalStop?.name));
        rowEl.appendChild(
          createJourneyResultCell("Araç Tipi", String(journey?.bus?.type || journey?.type || "").trim())
        );
        rowEl.appendChild(
          createJourneyResultCell("Fiyat", getJourneyPriceValue(journey?.price?.internet, journey?.price?.currency))
        );

        journeysResultsEl.appendChild(rowEl);
      });
    };

    const buildJourneySearchErrorMessage = (response, data, rawText) => {
      const lines = [];
      const baseMessage = getApiErrorMessage(response, data, "GetJourneys alınamadı");
      lines.push(baseMessage);

      const shouldHideRawPreview =
        response?.status === 401 ||
        (response?.redirected && /\/login(?:$|[?#])/.test(response.url || "")) ||
        /oturum süresi doldu/i.test(baseMessage);

      if (data?.step) {
        lines.push(`Adım: ${String(data.step)}`);
      }
      if (data?.requestUrl) {
        lines.push(`URL: ${String(data.requestUrl)}`);
      }
      if (data?.details) {
        lines.push(`Detay: ${String(data.details)}`);
      } else if (!shouldHideRawPreview) {
        const preview = normalizeRawErrorPreview(rawText);
        if (preview) {
          lines.push(`Ham yanıt: ${preview}`);
        }
      }

      return lines.join("\n");
    };

    const maybeTriggerGetJourneysRequest = () => {
      if (activeServiceKey !== "getjourneys") return;
      if (!companySelect.value) {
        setStatus("GetJourneys akışı için önce firma seçin.", "error");
        return;
      }
      if (!selectedJourneyOriginId || !selectedJourneyDestinationId) {
        setStatus("Kalkış ve varış istasyonlarını seçin.", "muted");
        return;
      }
      if (!selectedJourneysDate) {
        setStatus("GetJourneys için tarih seçin.", "muted");
        return;
      }
      performGetJourneysRequest();
    };

    const performGetJourneysRequest = async () => {
      const companyValue = String(companySelect.value || "").trim();
      const originId = selectedJourneyOriginId || String(journeysOriginSelect.value || "").trim();
      const destinationId = selectedJourneyDestinationId || String(journeysDestinationSelect.value || "").trim();
      const dateValue = String(journeysDateInput.value || "").trim();
      const dateRange = buildJourneysDateRange(dateValue);

      if (!companyValue) {
        setStatus("GetJourneys akışı için önce firma seçin.", "error");
        return;
      }
      if (!originId || !destinationId) {
        setStatus("Kalkış ve varış istasyonlarını seçin.", "error");
        return;
      }
      if (!dateRange) {
        setStatus("Geçerli bir tarih seçin.", "error");
        return;
      }

      const currentSequence = ++journeysRequestSequence;
      clearJourneysResults();
      setIoState("getjourneys", {
        requestBody: "İstek hazırlanıyor...",
        responseBody: "Response bekleniyor...",
        requestUrl: "",
        status: null
      });
      setStatus("GetJourneys isteği gönderiliyor...", "muted");

      try {
        const response = await fetch("/api/journey-search/journeys", {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            Accept: "application/json"
          },
          body: JSON.stringify({
            company: companyValue,
            origin: originId,
            destination: destinationId,
            date: dateValue
          })
        });
        const { data, rawText } = await parseJourneyStationsApiResponse(response);
        if (currentSequence !== journeysRequestSequence) return;

        const requestBodyValue = String(data?.requestBody || "").trim();
        const responseBodyValue = String(data?.responseBody || rawText || "").trim() || "Henüz response yok.";
        const requestUrlValue = String(data?.requestUrl || "").trim();
        const statusValue = data?.status ?? response.status ?? null;

        setIoState("getjourneys", {
          requestBody: requestBodyValue,
          responseBody: responseBodyValue,
          requestUrl: requestUrlValue,
          status: statusValue
        });
        renderJourneysResults(responseBodyValue, {
          originId,
          destinationId
        });

        if (!response.ok || !data?.ok) {
          throw new Error(buildJourneySearchErrorMessage(response, data, rawText));
        }

        setStatus("GetJourneys yanıtı alındı.", "success");
      } catch (err) {
        if (currentSequence !== journeysRequestSequence) return;
        setStatus(err?.message || "GetJourneys isteği başarısız.", "error");
      }
    };

    const normalizeRawErrorPreview = (text) =>
      String(text || "")
        .replace(/<[^>]+>/g, " ")
        .replace(/\s+/g, " ")
        .trim()
        .slice(0, 260);

    const parseJourneyStationsApiResponse = async (response) => {
      const rawText = await response.text();
      if (!rawText) {
        return {
          data: null,
          rawText: ""
        };
      }

      try {
        return {
          data: JSON.parse(rawText),
          rawText
        };
      } catch (err) {
        return {
          data: null,
          rawText
        };
      }
    };

    const buildJourneyStationsErrorMessage = (response, data, rawText) => {
      const lines = [];
      const baseMessage = getApiErrorMessage(response, data, "İstasyonlar alınamadı");
      lines.push(baseMessage);

      const shouldHideRawPreview =
        response?.status === 401 ||
        (response?.redirected && /\/login(?:$|[?#])/.test(response.url || "")) ||
        /oturum süresi doldu/i.test(baseMessage);

      if (data?.step) {
        lines.push(`Adım: ${String(data.step)}`);
      }
      if (data?.requestUrl) {
        lines.push(`URL: ${String(data.requestUrl)}`);
      }
      if (data?.details) {
        lines.push(`Detay: ${String(data.details)}`);
      } else if (!shouldHideRawPreview) {
        const preview = normalizeRawErrorPreview(rawText);
        if (preview) {
          lines.push(`Ham yanıt: ${preview}`);
        }
      }

      return lines.join("\n");
    };

    const getJourneyStationLabel = (item) => {
      const name = String(item?.name || item?.label || "").trim();
      const id = String(item?.id || "").trim();
      if (showIdCheckbox.checked && id && name) {
        return `${name} - ${id}`;
      }
      return name || id;
    };

    const fillSelect = (selectEl, items, placeholder, selectedValue = "") => {
      const normalizedItems = Array.isArray(items) ? items : [];
      const targetValue = String(selectedValue || "").trim();
      const fragment = document.createDocumentFragment();
      const placeholderOption = document.createElement("option");
      placeholderOption.value = "";
      placeholderOption.textContent = placeholder;
      fragment.appendChild(placeholderOption);

      let hasMatch = false;
      normalizedItems.forEach((item) => {
        const value = String(item?.value || "").trim();
        const label = getJourneyStationLabel(item);
        if (!value || !label) return;

        const option = document.createElement("option");
        option.value = value;
        option.textContent = label;
        if (targetValue && targetValue === value) {
          option.selected = true;
          hasMatch = true;
        }
        fragment.appendChild(option);
      });

      placeholderOption.selected = !hasMatch;
      selectEl.innerHTML = "";
      selectEl.appendChild(fragment);
    };

    const syncCompanySelection = (value) => {
      const normalizedValue = String(value || "").trim();
      companySelect.value = normalizedValue;
      journeysCompanySelect.value = normalizedValue;
    };

    const resetCompanyDependentSelections = () => {
      selectedOriginValue = "";
      selectedDestinationValue = "";
      selectedJourneyOriginId = "";
      selectedJourneyDestinationId = "";
      selectedJourneysDate = String(journeysDateInput.value || "").trim();
    };

    const resetStationSelects = (placeholder) => {
      loadedStationItems = [];
      selectedJourneyOriginId = "";
      selectedJourneyDestinationId = "";
      clearJourneysResults();
      fillSelect(originSelect, [], placeholder || "Önce firma seçin", "");
      fillSelect(destinationSelect, [], placeholder || "Önce firma seçin", "");
      renderJourneysStationSelects();
      syncCompanySelection(companySelect.value || journeysCompanySelect.value || "");
    };

    const renderLoadedStations = () => {
      if (!Array.isArray(loadedStationItems) || loadedStationItems.length === 0) return;
      fillSelect(originSelect, loadedStationItems, "Kalkış seçiniz", selectedOriginValue);
      fillSelect(destinationSelect, loadedStationItems, "Varış seçiniz", selectedDestinationValue);
      renderJourneysStationSelects();
    };

    const loadStations = async () => {
      const companyValue = String(companySelect.value || "").trim();
      const currentSequence = ++requestSequence;

      if (!companyValue || companyValue === "__partner_error__") {
        resetStationSelects("Önce firma seçin");
        setIoState("getstation", {
          requestBody: stationRequestBodyTemplate,
          responseBody: "Henüz response yok.",
          requestUrl: "",
          status: null
        });
        setStatus("Firma seçince kalkış ve varış listesi yüklenecek.", "muted");
        return;
      }

      fillSelect(originSelect, [], "İstasyonlar yükleniyor...", "");
      fillSelect(destinationSelect, [], "İstasyonlar yükleniyor...", "");
      setIoState("getstation", {
        requestBody: stationRequestBodyTemplate,
        responseBody: "Response bekleniyor...",
        requestUrl: "",
        status: null
      });
      setStatus("İstasyonlar yükleniyor...", "muted");

      try {
        const response = await fetch("/api/journey-search/stations", {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            Accept: "application/json"
          },
          body: JSON.stringify({
            company: companyValue
          })
        });
        const { data, rawText } = await parseJourneyStationsApiResponse(response);

        if (currentSequence !== requestSequence) return;
        setIoState("getstation", {
          requestBody: String(data?.requestBody || stationRequestBodyTemplate).trim() || stationRequestBodyTemplate,
          responseBody: String(data?.responseBody || rawText || "").trim() || "Henüz response yok.",
          requestUrl: String(data?.requestUrl || "").trim(),
          status: data?.status ?? response.status ?? null
        });
        if (!response.ok || !data?.ok || !Array.isArray(data.items)) {
          throw new Error(buildJourneyStationsErrorMessage(response, data, rawText));
        }

        const items = data.items
          .map((item) => ({
            value: String(item?.value || "").trim(),
            label: String(item?.label || "").trim(),
            id: String(item?.id || "").trim(),
            name: String(item?.name || item?.label || "").trim()
          }))
          .filter((item) => item.value && item.name);

        if (!items.length) {
          resetStationSelects("İstasyon bulunamadı");
          setStatus("Seçilen firma için istasyon bulunamadı.", "error");
          return;
        }

        loadedStationItems = items;
        renderLoadedStations();
        syncCompanySelection(companyValue);
        setStatus(`${items.length} istasyon yüklendi.`, "success");
      } catch (err) {
        if (currentSequence !== requestSequence) return;
        resetStationSelects("İstasyonlar yüklenemedi");
        setStatus(err?.message || "İstasyonlar alınamadı.", "error");
      }
    };

    companySelect.addEventListener("change", () => {
      syncCompanySelection(companySelect.value || "");
      resetCompanyDependentSelections();
      loadStations();
    });

    journeysCompanySelect.addEventListener("change", () => {
      syncCompanySelection(journeysCompanySelect.value || "");
      resetCompanyDependentSelections();
      loadStations();
    });

    originSelect.addEventListener("change", () => {
      selectedOriginValue = String(originSelect.value || "").trim();
      selectedJourneyOriginId = getStationSelectedId(originSelect);
      renderJourneysStationSelects();
      maybeTriggerGetJourneysRequest();
    });

    destinationSelect.addEventListener("change", () => {
      selectedDestinationValue = String(destinationSelect.value || "").trim();
      selectedJourneyDestinationId = getStationSelectedId(destinationSelect);
      renderJourneysStationSelects();
      maybeTriggerGetJourneysRequest();
    });

    showIdCheckbox.addEventListener("change", () => {
      renderLoadedStations();
    });

    journeysOriginSelect.addEventListener("change", () => {
      selectedJourneyOriginId = String(journeysOriginSelect.value || "").trim();
      maybeTriggerGetJourneysRequest();
    });

    journeysDestinationSelect.addEventListener("change", () => {
      selectedJourneyDestinationId = String(journeysDestinationSelect.value || "").trim();
      maybeTriggerGetJourneysRequest();
    });

    journeysDateInput.addEventListener("change", () => {
      selectedJourneysDate = String(journeysDateInput.value || "").trim();
      maybeTriggerGetJourneysRequest();
    });

    serviceButtons.forEach((button) => {
      button.addEventListener("click", () => {
        activateService(button.dataset.journeySearchServiceButton || "");
      });
    });

    activateService(serviceButtons[0]?.dataset?.journeySearchServiceButton || "getstation");

    syncCompanySelection(companySelect.value || journeysCompanySelect.value || "");
    if (String(companySelect.value || "").trim()) {
      loadStations();
    } else {
      resetStationSelects("Önce firma seçin");
      setIoState("getstation", {
        requestBody: stationRequestBodyTemplate,
        responseBody: "Henüz response yok.",
        requestUrl: "",
        status: null
      });
      setStatus("Firma seçince kalkış ve varış listesi yüklenecek.", "muted");
    }
  };

  const initObusRuleDefineWorkbench = () => {
    const root = document.querySelector("[data-obus-rule-define-page='1']");
    if (!root) return;
    if (root.dataset.bound === "1") return;
    root.dataset.bound = "1";

    const multiselect = root.querySelector(".obus-company-multiselect");
    const createRequestBaseUrl = String(root.getAttribute("data-obus-rule-create-base-url") || "").trim();
    const updateRequestBaseUrl = String(root.getAttribute("data-obus-rule-update-base-url") || "").trim();
    const createSubmitUrl =
      String(root.getAttribute("data-obus-rule-create-submit-url") || "").trim() || "/api/obus-rule-define/create";
    const updateSubmitUrl =
      String(root.getAttribute("data-obus-rule-update-submit-url") || "").trim() || "/api/obus-rule-define/update";
    const trigger = root.querySelector("#obus-rule-company-trigger");
    const dropdown = root.querySelector("#obus-rule-company-dropdown");
    const isAbroadFilterInput = root.querySelector("#obus-rule-isabroad-filter");
    const selectAllCheckbox = root.querySelector("[data-obus-rule-select-all='1']");
    const selectAllRow = root.querySelector(".obus-company-option-all");
    const companyCheckboxes = Array.from(root.querySelectorAll("[data-obus-rule-company-checkbox='1']"));
    const companyOptionRows = Array.from(root.querySelectorAll(".obus-company-option[data-obus-rule-company-option-row='1']"));
    const companyFilterEmptyEl = root.querySelector("[data-obus-rule-company-filter-empty='1']");
    const selectedCompaniesInput = root.querySelector("#obus-rule-selected-companies");
    const modeInput = root.querySelector("#obus-rule-mode");
    const modeButtons = Array.from(root.querySelectorAll("[data-obus-rule-mode-button]"));
    const modeDescriptionEl = root.querySelector("[data-obus-rule-mode-description='1']");
    const partnerRuleIdField = root.querySelector("[data-obus-rule-mode-only='update']");
    const partnerRuleIdInput = root.querySelector("#obus-rule-partner-rule-id");
    const startDateInput = root.querySelector("#obus-rule-start-date");
    const endDateInput = root.querySelector("#obus-rule-end-date");
    const rateInput = root.querySelector("#obus-rule-rate");
    const capacityBeginInput = root.querySelector("#obus-rule-capacity-begin");
    const capacityEndInput = root.querySelector("#obus-rule-capacity-end");
    const submitButton = root.querySelector("[data-obus-rule-submit='1']");
    const bodyPreviewEl = root.querySelector("[data-obus-rule-body-preview]");
    const requestPreviewEl = root.querySelector("[data-obus-rule-request-preview]");
    const statusEl = root.querySelector("[data-obus-rule-status='1']");
    let typeAheadText = "";
    let typeAheadTimerId = null;

    if (
      !bodyPreviewEl ||
      !requestPreviewEl ||
      !statusEl ||
      !selectedCompaniesInput ||
      !modeInput ||
      !startDateInput ||
      !endDateInput ||
      !rateInput ||
      !partnerRuleIdInput ||
      !capacityBeginInput ||
      !capacityEndInput ||
      !submitButton ||
      !isAbroadFilterInput ||
      !trigger ||
      !dropdown
    ) {
      return;
    }

    const parseJson = (raw, fallback) => {
      const text = String(raw || "").trim();
      if (!text) return fallback;
      try {
        return JSON.parse(text);
      } catch (err) {
        return fallback;
      }
    };

    const normalizeObusRuleMode = (value) => (String(value || "").trim().toLowerCase() === "update" ? "update" : "create");
    const obusRuleModeConfigs = {
      create: {
        key: "create",
        title: "Kural Ekle",
        actionName: "CreatePartnerRule",
        requestBaseUrl: createRequestBaseUrl,
        submitUrl: createSubmitUrl,
        requiresPartnerRuleId: false,
        description: "Seçilen firmalar için yeni partner kuralı oluşturma isteği hazırlayın.",
        idleMessage: "Firma, startDate, endDate, rate ve capacity alanlarını güncelledikçe JSON çıktıları yenilenir.",
        pendingMessage: "İstek henüz gönderilmedi.",
        loadingMessage: "CreatePartnerRule istekleri gönderiliyor...",
        readyMessage: (count) => `${count} firma için istek hazır. Göndermek için butonu kullanın.`,
        successMessage: (count) => `${count} firma için CreatePartnerRule isteği başarıyla tamamlandı.`,
        failureMessage: "CreatePartnerRule isteği başarısız.",
        transportErrorMessage: "CreatePartnerRule isteği gönderilemedi.",
        responseFallbackMessage: "CreatePartnerRule sonucu alınamadı"
      },
      update: {
        key: "update",
        title: "Kural Güncelle",
        actionName: "UpdatePartnerRule",
        requestBaseUrl: updateRequestBaseUrl,
        submitUrl: updateSubmitUrl,
        requiresPartnerRuleId: true,
        description: "Seçilen firmalar için partner-rule-id kullanarak kural güncelleme isteği hazırlayın.",
        idleMessage:
          "Firma, partnerRuleId, startDate, endDate, rate ve capacity alanlarını güncelledikçe JSON çıktıları yenilenir.",
        pendingMessage: "İstek henüz gönderilmedi.",
        loadingMessage: "UpdatePartnerRule istekleri gönderiliyor...",
        readyMessage: (count) => `${count} firma için güncelleme isteği hazır. Göndermek için butonu kullanın.`,
        successMessage: (count) => `${count} firma için UpdatePartnerRule isteği başarıyla tamamlandı.`,
        failureMessage: "UpdatePartnerRule isteği başarısız.",
        transportErrorMessage: "UpdatePartnerRule isteği gönderilemedi.",
        responseFallbackMessage: "UpdatePartnerRule sonucu alınamadı"
      }
    };
    const getActiveMode = () => normalizeObusRuleMode(modeInput.value);
    const getActiveModeConfig = () => obusRuleModeConfigs[getActiveMode()] || obusRuleModeConfigs.create;
    const buildIdleResponsePreview = (errorText = "") =>
      errorText
        ? { ok: false, error: String(errorText || "").trim() }
        : { ok: false, message: getActiveModeConfig().pendingMessage };
    const normalizeSearchText = (value) => String(value || "").toLocaleLowerCase("tr").trim();
    const clearTypeAhead = () => {
      typeAheadText = "";
      if (typeAheadTimerId) {
        clearTimeout(typeAheadTimerId);
        typeAheadTimerId = null;
      }
    };
    const queueTypeAheadReset = () => {
      if (typeAheadTimerId) {
        clearTimeout(typeAheadTimerId);
      }
      typeAheadTimerId = setTimeout(() => {
        typeAheadText = "";
        typeAheadTimerId = null;
      }, 900);
    };

    const readSelectedCompanyValues = () =>
      companyCheckboxes
        .filter((item) => item.checked)
        .map((item) => String(item.value || "").trim())
        .filter(Boolean);

    const readVisibleCompanyCheckboxes = () =>
      companyCheckboxes.filter((item) => {
        const row = item.closest("[data-obus-rule-company-option-row='1']");
        return row && row.hidden !== true && String(row.style.display || "").trim() !== "none";
      });

    const updateCompanyTriggerLabel = () => {
      const selectedValues = readSelectedCompanyValues();
      const visibleCompanyCheckboxes = readVisibleCompanyCheckboxes();
      if (selectedValues.length === 0) {
        trigger.textContent = "Firma seçiniz";
        return;
      }
      if (visibleCompanyCheckboxes.length > 0 && selectedValues.length === visibleCompanyCheckboxes.length) {
        trigger.textContent = "Hepsi";
        return;
      }
      if (selectedValues.length === 1) {
        const selectedItem = companyCheckboxes.find((item) => item.checked);
        trigger.textContent =
          String(selectedItem?.dataset.companyLabel || selectedItem?.closest("label")?.querySelector("span")?.textContent || "")
            .trim() || "1 firma seçildi";
        return;
      }
      trigger.textContent = `${selectedValues.length} firma seçildi`;
    };

    const syncSelectedCompanies = () => {
      const selectedValues = readSelectedCompanyValues();
      const visibleCompanyCheckboxes = readVisibleCompanyCheckboxes();
      if (selectAllCheckbox) {
        selectAllCheckbox.checked = selectedValues.length > 0 && selectedValues.length === visibleCompanyCheckboxes.length;
      }
      selectedCompaniesInput.value = JSON.stringify(selectedValues);
      updateCompanyTriggerLabel();
    };

    const applyInitialCompanySelection = () => {
      const parsed = parseJson(selectedCompaniesInput.value, []);
      const initialValues = Array.isArray(parsed)
        ? parsed.map((item) => String(item || "").trim()).filter(Boolean)
        : [];
      const allowedValues = new Set(companyCheckboxes.map((item) => String(item.value || "").trim()).filter(Boolean));
      const normalizedInitial = initialValues.filter((value) => allowedValues.has(value));
      const selectedSet = new Set(normalizedInitial);

      companyCheckboxes.forEach((item) => {
        item.checked = selectedSet.has(String(item.value || "").trim());
      });
      syncSelectedCompanies();
    };

    const applyModeUi = (nextMode) => {
      const normalizedMode = normalizeObusRuleMode(nextMode);
      modeInput.value = normalizedMode;
      const modeConfig = getActiveModeConfig();

      modeButtons.forEach((button) => {
        const buttonMode = normalizeObusRuleMode(button.getAttribute("data-obus-rule-mode-button"));
        const isActive = buttonMode === normalizedMode;
        button.classList.toggle("is-active", isActive);
        button.setAttribute("aria-pressed", isActive ? "true" : "false");
      });

      if (partnerRuleIdField) {
        const showPartnerRuleId = modeConfig.requiresPartnerRuleId === true;
        partnerRuleIdField.hidden = !showPartnerRuleId;
        partnerRuleIdField.style.display = showPartnerRuleId ? "" : "none";
      }
      if (partnerRuleIdInput) {
        partnerRuleIdInput.disabled = modeConfig.requiresPartnerRuleId !== true;
      }
      if (modeDescriptionEl) {
        modeDescriptionEl.textContent = modeConfig.description;
      }
    };

    const closeDropdown = () => {
      dropdown.hidden = true;
      trigger.setAttribute("aria-expanded", "false");
      clearTypeAhead();
    };

    const openDropdown = () => {
      dropdown.hidden = false;
      trigger.setAttribute("aria-expanded", "true");
    };

    const readCompanyIsAbroadValue = (row, checkbox) =>
      String(
        checkbox?.dataset.companyIsabroad ||
          row?.dataset.companyIsabroad ||
          row?.getAttribute("data-company-isabroad") ||
          ""
      )
        .trim()
        .toLowerCase();

    const setCompanyRowVisibility = (row, isVisible) => {
      if (!row) return;
      row.hidden = !isVisible;
      row.style.display = isVisible ? "" : "none";
    };

    const applyCompanyFilter = () => {
      const normalizedFilter = String(isAbroadFilterInput.value || "all").trim().toLowerCase();
      let visibleCount = 0;

      companyOptionRows.forEach((row) => {
        const checkbox = row.querySelector("[data-obus-rule-company-checkbox='1']");
        const rowIsAbroad = readCompanyIsAbroadValue(row, checkbox);
        const isMatch = normalizedFilter === "all" || rowIsAbroad === normalizedFilter;
        setCompanyRowVisibility(row, isMatch);
        if (!isMatch && checkbox) {
          checkbox.checked = false;
        }
        if (isMatch) {
          visibleCount += 1;
        }
      });

      if (selectAllRow) {
        selectAllRow.hidden = visibleCount === 0;
        selectAllRow.style.display = visibleCount === 0 ? "none" : "";
      }
      if (companyFilterEmptyEl) {
        companyFilterEmptyEl.hidden = visibleCount > 0;
        companyFilterEmptyEl.style.display = visibleCount > 0 ? "none" : "";
      }
      syncSelectedCompanies();
      return visibleCount;
    };

    const findMatchingCompanyOption = (queryText) => {
      const normalizedQuery = normalizeSearchText(queryText);
      if (!normalizedQuery) return null;
      const optionRows = companyOptionRows.filter(
        (row) => row.hidden !== true && String(row.style.display || "").trim() !== "none"
      );
      return (
        optionRows.find((row) => {
          const labelText = String(row.querySelector("span")?.textContent || "");
          return normalizeSearchText(labelText).includes(normalizedQuery);
        }) || null
      );
    };

    const focusCompanyOptionRow = (row) => {
      if (!row) return;
      const currentFocusedRows = Array.from(root.querySelectorAll(".obus-company-option-focus"));
      currentFocusedRows.forEach((item) => item.classList.remove("obus-company-option-focus"));
      row.scrollIntoView({ block: "nearest" });
      row.classList.add("obus-company-option-focus");
      setTimeout(() => {
        row.classList.remove("obus-company-option-focus");
      }, 450);
    };

    const handleTypeAheadKey = (event) => {
      if (event.metaKey || event.ctrlKey || event.altKey) return;
      if (event.key === "Escape" || event.key === "Tab") return;

      let nextQuery = typeAheadText;
      if (event.key === "Backspace") {
        event.preventDefault();
        nextQuery = String(typeAheadText || "").slice(0, -1);
      } else if (event.key.length === 1) {
        event.preventDefault();
        nextQuery = `${String(typeAheadText || "")}${event.key}`;
      } else {
        return;
      }

      typeAheadText = normalizeSearchText(nextQuery);
      queueTypeAheadReset();
      if (!typeAheadText) return;

      if (dropdown.hidden) {
        openDropdown();
      }
      focusCompanyOptionRow(findMatchingCompanyOption(typeAheadText));
    };

    const readSelectedCompanies = () =>
      companyCheckboxes
        .filter((item) => item.checked)
        .map((item) => ({
          value: String(item.value || "").trim(),
          label: String(item.dataset.companyLabel || "").trim(),
          code: String(item.dataset.companyCode || "").trim(),
          id: String(item.dataset.companyId || "").trim(),
          cluster: String(item.dataset.companyCluster || "").trim(),
          branchId: String(item.dataset.companyBranchId || "").trim(),
          url: String(item.dataset.companyUrl || "").trim()
        }))
        .filter((item) => item.value && item.code);

    const parseIntegerFieldState = (value) => {
      const text = String(value || "").trim();
      if (!text) {
        return {
          text: "",
          value: null,
          valid: true,
          hasValue: false
        };
      }
      if (!/^-?\d+$/.test(text)) {
        return {
          text,
          value: null,
          valid: false,
          hasValue: true
        };
      }
      const parsed = Number.parseInt(text, 10);
      return {
        text,
        value: Number.isInteger(parsed) ? parsed : null,
        valid: Number.isInteger(parsed),
        hasValue: true
      };
    };

    const readFormState = () => {
      const mode = getActiveMode();
      const selectedCompanies = readSelectedCompanies();
      const startDate = String(startDateInput.value || "").trim();
      const endDate = String(endDateInput.value || "").trim();
      const rateText = String(rateInput.value || "").trim().replace(",", ".");
      const parsedRate = Number.parseFloat(rateText);
      const capacityBeginState = parseIntegerFieldState(capacityBeginInput.value);
      const capacityEndState = parseIntegerFieldState(capacityEndInput.value);
      const partnerRuleIdState = parseIntegerFieldState(partnerRuleIdInput.value);

      return {
        mode,
        modeConfig: getActiveModeConfig(),
        selectedCompanies,
        startDate,
        endDate,
        rateText,
        parsedRate,
        capacityBeginState,
        capacityEndState,
        partnerRuleIdState
      };
    };

    const normalizeIsoDateInput = (value) => {
      const text = String(value || "").trim();
      return /^\d{4}-\d{2}-\d{2}$/.test(text) ? text : "";
    };

    const buildIsoDateText = (value, endOfDay = false) => {
      const normalized = normalizeIsoDateInput(value);
      if (!normalized) return "";
      const date = new Date(`${normalized}T${endOfDay ? "23:59:59.999" : "00:00:00.000"}Z`);
      if (Number.isNaN(date.getTime())) return "";
      return date.toISOString();
    };

    const normalizeObusClusterLabel = (value) => {
      const normalized = String(value || "").trim().toLowerCase();
      return /^cluster\d+$/.test(normalized) ? normalized : "";
    };

    const buildRequestUrl = (clusterLabel, mode = getActiveMode()) => {
      const modeConfig = obusRuleModeConfigs[normalizeObusRuleMode(mode)] || obusRuleModeConfigs.create;
      const requestBaseUrl = String(modeConfig.requestBaseUrl || "").trim();
      const normalizedCluster = normalizeObusClusterLabel(clusterLabel);
      if (!requestBaseUrl || !normalizedCluster || !/cluster\d+/i.test(requestBaseUrl)) {
        return "";
      }
      const nextUrl = requestBaseUrl.replace(/cluster\d+/i, normalizedCluster);
      const matchedCluster = normalizeObusClusterLabel((String(nextUrl).match(/cluster\d+/i) || [])[0] || "");
      return matchedCluster === normalizedCluster ? nextUrl : "";
    };

    const buildCreateRequestBodyForCompany = (company, { usePlaceholders = true, formState = readFormState() } = {}) => {
      return {
        data: {
          "partner-id": Number.parseInt(String(company?.id || "0").trim(), 10) || 0,
          "rule-id": 2,
          data: {
            StartDate: buildIsoDateText(formState.startDate, false),
            StartTime: "00:00",
            EndDate: buildIsoDateText(formState.endDate, true),
            EndTime: "23:59",
            BranchType: 1,
            CapacityBegin: formState.capacityBeginState.hasValue ? formState.capacityBeginState.value : formState.capacityBeginState.text,
            CapacityEnd: formState.capacityEndState.hasValue ? formState.capacityEndState.value : formState.capacityEndState.text,
            IsActive: true,
            PriceChange: "Decrease",
            Rate: Number.isFinite(formState.parsedRate) ? formState.parsedRate : formState.rateText,
            Weekdays: "Monday,Tuesday,Wednesday,Thursday,Friday,Saturday,Sunday",
            IncludedBranches: "",
            IgnoredBranches: "",
            IncludedRoutes: "",
            IgnoredRoutes: "",
            IncludedUsers: "",
            IgnoredUsers: ""
          },
          "partner-rule-station": []
        },
        "device-session": {
          "session-id": usePlaceholders ? "{{sessionId}}" : "",
          "device-id": usePlaceholders ? "{{deviceId}}" : ""
        },
        date: new Date().toISOString().slice(0, 19).replace("T", " "),
        language: "tr-TR",
        token: usePlaceholders ? "{{token}}" : ""
      };
    };

    const buildUpdateRequestBodyForCompany = (company, { usePlaceholders = true, formState = readFormState() } = {}) => ({
      data: {
        "partner-id": Number.parseInt(String(company?.id || "0").trim(), 10) || 0,
        "partner-rule-id": formState.partnerRuleIdState.hasValue ? formState.partnerRuleIdState.value : 0,
        data: {
          StartDate: buildIsoDateText(formState.startDate, false),
          StartTime: "00:00",
          EndDate: buildIsoDateText(formState.endDate, true),
          EndTime: "23:59",
          Description: null,
          BranchType: 1,
          CapacityBegin: formState.capacityBeginState.hasValue ? formState.capacityBeginState.value : null,
          CapacityEnd: formState.capacityEndState.hasValue ? formState.capacityEndState.value : null,
          IsActive: true,
          PriceChange: "Decrease",
          MinutesToDepartureTime: null,
          Rate: Number.isFinite(formState.parsedRate) ? formState.parsedRate : null,
          Weekdays: "Monday,Tuesday,Wednesday,Thursday,Friday,Saturday,Sunday",
          IncludedBranches: "",
          IgnoredBranches: "",
          IncludedRoutes: "",
          IgnoredRoutes: "",
          IncludedUsers: "",
          IgnoredUsers: ""
        },
        "partner-rule-station": []
      },
      "device-session": {
        "session-id": usePlaceholders ? "{{sessionId}}" : "",
        "device-id": usePlaceholders ? "{{deviceId}}" : ""
      },
      date: new Date().toISOString().slice(0, 19).replace("T", " "),
      language: "tr-TR",
      token: usePlaceholders ? "{{token}}" : ""
    });

    const buildValidationState = () => {
      const formState = readFormState();
      const {
        mode,
        selectedCompanies,
        startDate,
        endDate,
        parsedRate,
        capacityBeginState,
        capacityEndState,
        partnerRuleIdState
      } = formState;

      if (selectedCompanies.length === 0) {
        return { ok: false, error: "En az bir firma seçmelisiniz." };
      }
      if (mode === "update") {
        if (!partnerRuleIdState.valid || !partnerRuleIdState.hasValue || !Number.isInteger(partnerRuleIdState.value)) {
          return { ok: false, error: "Geçerli bir PartnerRuleId girilmelidir." };
        }
        if (partnerRuleIdState.value <= 0) {
          return { ok: false, error: "PartnerRuleId 0'dan büyük olmalıdır." };
        }
      }
      if (!normalizeIsoDateInput(startDate) || !normalizeIsoDateInput(endDate)) {
        return { ok: false, error: "StartDate ve EndDate zorunludur." };
      }
      if (String(startDate) > String(endDate)) {
        return { ok: false, error: "StartDate, EndDate'ten büyük olamaz." };
      }
      if (!Number.isFinite(parsedRate)) {
        return { ok: false, error: "Geçerli bir Rate girilmelidir." };
      }
      if (mode === "create") {
        if (!capacityBeginState.valid || !capacityBeginState.hasValue || !Number.isInteger(capacityBeginState.value) || capacityBeginState.value < 0) {
          return { ok: false, error: "Geçerli bir CapacityBegin girilmelidir." };
        }
        if (!capacityEndState.valid || !capacityEndState.hasValue || !Number.isInteger(capacityEndState.value) || capacityEndState.value < 0) {
          return { ok: false, error: "Geçerli bir CapacityEnd girilmelidir." };
        }
      } else {
        if (capacityBeginState.hasValue && (!capacityBeginState.valid || !Number.isInteger(capacityBeginState.value) || capacityBeginState.value < 0)) {
          return { ok: false, error: "CapacityBegin girilmişse geçerli bir sayı olmalıdır." };
        }
        if (capacityEndState.hasValue && (!capacityEndState.valid || !Number.isInteger(capacityEndState.value) || capacityEndState.value < 0)) {
          return { ok: false, error: "CapacityEnd girilmişse geçerli bir sayı olmalıdır." };
        }
      }
      if (
        capacityBeginState.hasValue &&
        capacityEndState.hasValue &&
        Number.isInteger(capacityBeginState.value) &&
        Number.isInteger(capacityEndState.value) &&
        capacityBeginState.value > capacityEndState.value
      ) {
        return { ok: false, error: "CapacityBegin, CapacityEnd'ten büyük olamaz." };
      }
      return {
        ok: true,
        mode,
        companyCount: selectedCompanies.length,
        startDate,
        endDate,
        rate: parsedRate,
        capacityBegin: capacityBeginState.value,
        capacityEnd: capacityEndState.value,
        partnerRuleId: partnerRuleIdState.value
      };
    };

    const buildBodyPreviewPayload = () => {
      const formState = readFormState();
      const selectedCompanies = formState.selectedCompanies;
      const mode = formState.mode;
      const buildRequestBodyForCompany = mode === "update" ? buildUpdateRequestBodyForCompany : buildCreateRequestBodyForCompany;
      if (selectedCompanies.length === 0) return [];
      const entries = selectedCompanies.map((company) => {
        const payload = buildRequestBodyForCompany(company, { usePlaceholders: true, formState });
        if (selectedCompanies.length === 1) {
          return payload;
        }
        return {
          company: company.code,
          requestUrl: buildRequestUrl(company.cluster, mode),
          body: payload
        };
      });
      return entries.length === 1 ? entries[0] : entries;
    };

    const buildResponseEntry = (item) => {
      const responseBody = item?.responseBody;
      if (responseBody && typeof responseBody === "object") {
        return responseBody;
      }
      if (typeof responseBody === "string" && responseBody.trim()) {
        return {
          ok: item?.ok === true,
          status: Number.isFinite(Number(item?.status)) ? Number(item.status) : null,
          raw: responseBody
        };
      }
      return {
        ok: item?.ok === true,
        status: Number.isFinite(Number(item?.status)) ? Number(item.status) : null,
        message: String(item?.message || "").trim(),
        error: String(item?.error || "").trim(),
        errorDetail: String(item?.errorDetail || "").trim()
      };
    };

    const buildResponsePreviewPayload = (payload) => {
      const results = Array.isArray(payload?.results) ? payload.results : [];
      if (results.length === 0) {
        return payload;
      }
      const entries = results.map((item) => {
        if (results.length === 1) {
          return buildResponseEntry(item);
        }
        return {
          company: String(item?.company || "").trim(),
          requestUrl: String(item?.requestUrl || "").trim(),
          response: buildResponseEntry(item)
        };
      });
      return entries.length === 1 ? entries[0] : entries;
    };

    const updateStatus = (validationState, options = {}) => {
      const modeConfig = getActiveModeConfig();
      if (options.loading) {
        statusEl.textContent = modeConfig.loadingMessage;
        statusEl.className = "obus-rule-define-status muted";
        return;
      }
      if (validationState?.ok) {
        statusEl.textContent = modeConfig.readyMessage(Number(validationState.companyCount || 0));
        statusEl.className = "obus-rule-define-status inline-success";
        return;
      }
      if (validationState?.error) {
        statusEl.textContent = validationState.error;
        statusEl.className = "obus-rule-define-status alert inline-alert";
        return;
      }
      statusEl.textContent = modeConfig.idleMessage;
      statusEl.className = "obus-rule-define-status muted";
    };

    const updatePreview = ({ preserveResponse = false } = {}) => {
      const validationState = buildValidationState();
      const bodyPreviewPayload = buildBodyPreviewPayload();
      bodyPreviewEl.textContent = JSON.stringify(bodyPreviewPayload, null, 2);
      if (!preserveResponse) {
        requestPreviewEl.textContent = JSON.stringify(
          buildIdleResponsePreview(validationState.ok ? "" : validationState.error),
          null,
          2
        );
      }
      updateStatus(validationState);
      return validationState;
    };

    trigger.addEventListener("click", (event) => {
      event.preventDefault();
      if (dropdown.hidden) {
        openDropdown();
      } else {
        closeDropdown();
      }
    });

    trigger.addEventListener("keydown", (event) => {
      handleTypeAheadKey(event);
    });

    document.addEventListener("click", (event) => {
      if (!multiselect || multiselect.contains(event.target)) return;
      closeDropdown();
    });

    document.addEventListener("keydown", (event) => {
      if (event.key === "Escape") {
        closeDropdown();
      }
    });

    if (multiselect) {
      multiselect.addEventListener("keydown", (event) => {
        const activeElement = document.activeElement;
        const isCheckboxActive =
          activeElement instanceof HTMLInputElement && activeElement.type === "checkbox";
        if (!isCheckboxActive) return;
        handleTypeAheadKey(event);
      });
    }

    submitButton.addEventListener("click", async () => {
      const modeConfig = getActiveModeConfig();
      const validationState = updatePreview({ preserveResponse: true });
      if (!validationState.ok) return;

      closeDropdown();
      submitButton.disabled = true;
      updateStatus(validationState, { loading: true });

      try {
        const response = await fetch(modeConfig.submitUrl, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            Accept: "application/json"
          },
          body: JSON.stringify({
            selectedCompanies: readSelectedCompanyValues(),
            startDate: String(startDateInput.value || "").trim(),
            endDate: String(endDateInput.value || "").trim(),
            rate: String(rateInput.value || "").trim(),
            partnerRuleId: String(partnerRuleIdInput.value || "").trim(),
            capacityBegin: String(capacityBeginInput.value || "").trim(),
            capacityEnd: String(capacityEndInput.value || "").trim()
          })
        });
        const data = await parseJsonResponse(response);
        const payload = data || {
          ok: false,
          error: getApiErrorMessage(response, data, modeConfig.responseFallbackMessage)
        };
        requestPreviewEl.textContent = JSON.stringify(buildResponsePreviewPayload(payload), null, 2);

        if (!response.ok || payload?.ok === false) {
          if (payload?.successCount > 0 || payload?.failureCount > 0) {
            statusEl.textContent = `${Number(payload.successCount || 0)} başarılı, ${Number(payload.failureCount || 0)} başarısız istek tamamlandı.`;
            statusEl.className = "obus-rule-define-status alert inline-alert";
          } else {
            statusEl.textContent = String(payload?.error || modeConfig.failureMessage).trim();
            statusEl.className = "obus-rule-define-status alert inline-alert";
          }
          return;
        }

        statusEl.textContent = modeConfig.successMessage(Number(payload.successCount || 0));
        statusEl.className = "obus-rule-define-status inline-success";
      } catch (err) {
        requestPreviewEl.textContent = JSON.stringify(
          {
            ok: false,
            error: String(err?.message || modeConfig.transportErrorMessage).trim()
          },
          null,
          2
        );
        statusEl.textContent = String(err?.message || modeConfig.transportErrorMessage).trim();
        statusEl.className = "obus-rule-define-status alert inline-alert";
      } finally {
        submitButton.disabled = false;
      }
    });

    selectAllCheckbox?.addEventListener("change", () => {
      readVisibleCompanyCheckboxes().forEach((item) => {
        item.checked = Boolean(selectAllCheckbox.checked);
      });
      syncSelectedCompanies();
      updatePreview();
    });

    companyCheckboxes.forEach((item) => {
      item.addEventListener("change", () => {
        syncSelectedCompanies();
        updatePreview();
      });
    });

    [partnerRuleIdInput, startDateInput, endDateInput, rateInput, capacityBeginInput, capacityEndInput].forEach((input) => {
      input.addEventListener("input", updatePreview);
      input.addEventListener("change", updatePreview);
    });

    isAbroadFilterInput.addEventListener("change", () => {
      applyCompanyFilter();
      updatePreview();
    });

    modeButtons.forEach((button) => {
      button.addEventListener("click", () => {
        applyModeUi(button.getAttribute("data-obus-rule-mode-button"));
        updatePreview();
      });
    });

    applyModeUi(getActiveMode());
    applyInitialCompanySelection();
    applyCompanyFilter();
    updatePreview();
  };

  const initObusUserCreateWorkbench = () => {
    const root = document.querySelector("[data-obus-user-create-page='1']");
    if (!root) return;
    if (root.dataset.bound === "1") return;
    root.dataset.bound = "1";

    const listUrl = String(root.getAttribute("data-obus-user-create-template-list-url") || "").trim();
    const saveUrl = String(root.getAttribute("data-obus-user-create-template-save-url") || "").trim();
    const detailBaseUrl = String(root.getAttribute("data-obus-user-create-template-detail-base-url") || "").trim();
    const deleteBaseUrl = String(root.getAttribute("data-obus-user-create-template-delete-base-url") || "").trim();
    const submitUrl = String(root.getAttribute("data-obus-user-create-submit-url") || "").trim();
    const companyCountRaw = Number.parseInt(String(root.getAttribute("data-obus-user-create-company-count") || "0"), 10);
    const companyCount = Number.isFinite(companyCountRaw) && companyCountRaw > 0 ? companyCountRaw : 0;
    const samplePartnerIdRaw = Number.parseInt(
      String(root.getAttribute("data-obus-user-create-sample-partner-id") || "0"),
      10
    );
    const sampleBranchIdRaw = Number.parseInt(
      String(root.getAttribute("data-obus-user-create-sample-branch-id") || "0"),
      10
    );
    const samplePartnerId = Number.isFinite(samplePartnerIdRaw) && samplePartnerIdRaw > 0 ? samplePartnerIdRaw : 0;
    const sampleBranchId = Number.isFinite(sampleBranchIdRaw) && sampleBranchIdRaw > 0 ? sampleBranchIdRaw : 0;
    const rowsContainer = root.querySelector("[data-obus-user-create-rows='1']");
    const rowTemplate = root.querySelector("#obus-user-create-row-template");
    const templateNameInput = root.querySelector("[data-obus-user-create-template-name='1']");
    const templateSelect = root.querySelector("[data-obus-user-create-template-select='1']");
    const addRowButton = root.querySelector("[data-obus-user-create-add-row='1']");
    const clearRowsButton = root.querySelector("[data-obus-user-create-clear-rows='1']");
    const createUsersButton = root.querySelector("[data-obus-user-create-submit='1']");
    const saveTemplateButton = root.querySelector("[data-obus-user-create-save-template='1']");
    const refreshTemplatesButton = root.querySelector("[data-obus-user-create-refresh-templates='1']");
    const loadTemplateButton = root.querySelector("[data-obus-user-create-load-template='1']");
    const deleteTemplateButton = root.querySelector("[data-obus-user-create-delete-template='1']");
    const statusEl = root.querySelector("[data-obus-user-create-status='1']");
    const rowCountEl = root.querySelector("[data-obus-user-create-row-count='1']");
    const previewEl = root.querySelector("[data-obus-user-create-preview='1']");
    const responseSummaryEl = root.querySelector("[data-obus-user-create-response-summary='1']");
    const responseListEl =
      root.parentElement?.querySelector("[data-obus-user-create-response-list='1']") ||
      document.querySelector("[data-obus-user-create-response-list='1']");

    if (
      !listUrl ||
      !saveUrl ||
      !detailBaseUrl ||
      !deleteBaseUrl ||
      !submitUrl ||
      !rowsContainer ||
      !rowTemplate ||
      !templateNameInput ||
      !templateSelect ||
      !addRowButton ||
      !clearRowsButton ||
      !createUsersButton ||
      !saveTemplateButton ||
      !refreshTemplatesButton ||
      !loadTemplateButton ||
      !deleteTemplateButton ||
      !statusEl ||
      !rowCountEl ||
      !previewEl ||
      !responseSummaryEl ||
      !responseListEl
    ) {
      return;
    }

    let currentTemplateId = null;
    let templates = [];
    let pendingRequests = 0;
    let createJobRunning = false;
    let activeJobId = "";
    let activeJobCursor = 0;
    let activeJobPollTimerId = 0;
    let activeJobEvents = [];
    let activeJobFailureCount = 0;
    let activeJobCreatedAt = 0;
    let activeJobFinishedAt = 0;

    const permissionTypes = [
      "CanSeePassengerInformation",
      "CanSeeAgentName",
      "CanSearchTickets",
      "CanViewExpiredJourney",
      "CanViewJourneyActivity",
      "CanViewCancelledJourney",
      "CanRefundOpenTicket",
      "CanMatchSidelinedTicketToJourney",
      "IgnoreMaximumSalesParameters",
      "CanTransferAtOtherBranch",
      "CanEditOnlineTicket",
      "CanRefundOnlineTicket",
      "AllowRefundOptionExpiredTicketsForTransfer",
      "AllowRefundOptionExpiredTickets",
      "CanRefundOtherSalesAtOwnBranch",
      "CanRefundOwnSalesAtOwnBranch",
      "CanTransferAtOwnBranch",
      "CanRefundObiletTicket",
      "CanRefundWebTicket",
      "PermittedAllBranchStations"
    ];

    const createEmptyEntry = () => ({
      fullName: "",
      username: "",
      password: ""
    });

    const normalizeTemplateName = (value) =>
      String(value || "")
        .trim()
        .replace(/\s+/g, " ")
        .slice(0, 120);

    const normalizeEntryText = (value) => String(value || "").trim().slice(0, 160);
    const normalizeEntryPassword = (value) => String(value || "").slice(0, 160);

    const isBusy = () => pendingRequests > 0 || createJobRunning;

    const getRows = () => Array.from(rowsContainer.querySelectorAll("[data-obus-user-create-row='1']"));

    const readEntries = () =>
      getRows().map((row) => ({
        fullName: normalizeEntryText(row.querySelector("[data-obus-user-create-input='fullName']")?.value || ""),
        username: normalizeEntryText(row.querySelector("[data-obus-user-create-input='username']")?.value || ""),
        password: normalizeEntryPassword(row.querySelector("[data-obus-user-create-input='password']")?.value || "")
      }));

    const readFilledEntries = () =>
      readEntries().filter((entry) => entry.fullName || entry.username || entry.password);

    const setStatus = (message, tone = "muted") => {
      statusEl.textContent = String(message || "").trim();
      statusEl.classList.remove("is-error", "is-success");
      if (tone === "error") {
        statusEl.classList.add("is-error");
      } else if (tone === "success") {
        statusEl.classList.add("is-success");
      }
    };

    const buildPermissionList = (branchIdValue) =>
      permissionTypes.map((type) => ({
        "branch-id": branchIdValue,
        type,
        "is-deleted": false,
        "user-id": 0
      }));

    const buildSampleRequestBody = (entry) => ({
      data: {
        "full-name": String(entry?.fullName || "").trim(),
        "is-active": true,
        "day-for-can-view-expired-journey": null,
        email: null,
        notes: null,
        password: String(entry?.password || ""),
        phone: "9999999999",
        username: String(entry?.username || "").trim(),
        "ignore-password-check": false,
        id: 0,
        "is-system-user": false,
        "user-modules": [
          {
            "module-id": "Obus",
            "partner-id": samplePartnerId || 0,
            "user-id": 0
          }
        ],
        branches: [sampleBranchId || 0],
        "time-to-change-password": 0,
        "is-mac-address-check": false,
        permissions: buildPermissionList(sampleBranchId || 0),
        "report-permissions": [],
        "branch-station-permission": [],
        "user-branch-profile": []
      },
      "device-session": {
        "session-id": "{{sessionId}}",
        "device-id": "{{deviceId}}"
      },
      language: "tr-TR",
      token: "{{token}}"
    });

    const validateEntriesForSubmit = () => {
      const rows = readEntries();
      const validEntries = [];
      const incompleteRows = [];

      rows.forEach((entry, index) => {
        const hasFullName = Boolean(entry.fullName);
        const hasUsername = Boolean(entry.username);
        const hasPassword = Boolean(entry.password);
        const filledCount = [hasFullName, hasUsername, hasPassword].filter(Boolean).length;
        if (filledCount === 0) return;
        if (filledCount < 3) {
          incompleteRows.push(index + 1);
          return;
        }
        validEntries.push(entry);
      });

      if (incompleteRows.length > 0) {
        return {
          ok: false,
          error: `Bazı satırlar eksik. Ad Soyad, Kullanıcı Adı ve Şifre alanlarının tamamı doldurulmalıdır. Satır: ${incompleteRows.join(", ")}`
        };
      }

      if (!validEntries.length) {
        return {
          ok: false,
          error: "En az bir kullanıcı satırı doldurulmalıdır."
        };
      }

      return {
        ok: true,
        entries: validEntries
      };
    };

    const syncActionState = () => {
      const disabled = isBusy();
      templateNameInput.disabled = disabled;
      templateSelect.disabled = disabled;
      addRowButton.disabled = disabled;
      clearRowsButton.disabled = disabled;
      createUsersButton.disabled = disabled;
      saveTemplateButton.disabled = disabled;
      refreshTemplatesButton.disabled = disabled;
      loadTemplateButton.disabled = disabled || !String(templateSelect.value || "").trim();
      deleteTemplateButton.disabled = disabled || !String(templateSelect.value || "").trim();
      getRows().forEach((row) => {
        row.querySelectorAll("input").forEach((input) => {
          input.disabled = disabled;
        });
        const removeButton = row.querySelector("[data-obus-user-create-remove-row='1']");
        if (removeButton) {
          removeButton.disabled = disabled;
        }
      });
    };

    const setBusy = (nextBusy) => {
      pendingRequests = nextBusy ? pendingRequests + 1 : Math.max(0, pendingRequests - 1);
      syncActionState();
    };

    const syncRowMeta = () => {
      const rows = getRows();
      const filledCount = readFilledEntries().length;
      rows.forEach((row, index) => {
        const indexBadge = row.querySelector("[data-obus-user-create-row-index='1']");
        if (indexBadge) {
          indexBadge.textContent = `Satır ${index + 1}`;
        }
      });
      rowCountEl.textContent = `${rows.length} satır / ${filledCount} dolu`;
    };

    const buildLiveEventTone = (event = null) => {
      const statusKind = String(event?.statusKind || "")
        .trim()
        .toLocaleLowerCase("tr");
      if (statusKind === "failure" || event?.ok === false) return "error";
      if (statusKind === "success" || event?.ok === true) return "success";
      return "pending";
    };

    const buildLiveEventStateText = (event = null) => {
      const statusKind = String(event?.statusKind || "")
        .trim()
        .toLocaleLowerCase("tr");
      if (statusKind === "failure" || event?.ok === false) return "Hatalı";
      if (statusKind === "success" || event?.ok === true) return "Başarılı";
      if (statusKind === "pending") return "Hazırlanıyor";
      if (statusKind === "progress") return "İşleniyor";
      if (statusKind === "info") return "Bilgi";
      return "Beklemede";
    };

    const buildLiveEventRows = () => {
      const eventMap = new Map();
      activeJobEvents.forEach((event) => {
        const key = String(event?.key || "").trim() || `event-${Number(event?.seq || 0)}`;
        eventMap.set(key, event);
      });
      return Array.from(eventMap.values());
    };

    const normalizeLiveMessageText = (value) => String(value || "").replace(/\s+/g, " ").trim();

    const getLiveEventMessageText = (event = null, { includePending = true } = {}) => {
      const explicitText = normalizeLiveMessageText(String(event?.message || "").trim() || String(event?.error || "").trim());
      if (explicitText) return explicitText;
      if (includePending && buildLiveEventTone(event) === "pending") return "İstek akışı devam ediyor.";
      return "";
    };

    const isDuplicateUserMessage = (messageText = "") =>
      normalizeLiveMessageText(messageText)
        .toLocaleLowerCase("tr")
        .includes("bu kullanıcı isimli kullanıcı daha önceden sisteme kayıt olmuştur");

    const formatLiveDuration = (durationMs) => {
      const safeMs = Number.isFinite(Number(durationMs)) ? Math.max(0, Number(durationMs)) : 0;
      const totalSeconds = Math.floor(safeMs / 1000);
      const hours = Math.floor(totalSeconds / 3600);
      const minutes = Math.floor((totalSeconds % 3600) / 60);
      const seconds = totalSeconds % 60;

      if (hours > 0) {
        return `${hours} sa ${String(minutes).padStart(2, "0")} dk ${String(seconds).padStart(2, "0")} sn`;
      }
      if (minutes > 0) {
        return `${minutes} dk ${String(seconds).padStart(2, "0")} sn`;
      }
      return `${seconds} sn`;
    };

    const parseLiveEventMeta = (event = null) => {
      const keyParts = String(event?.key || "").split("|||");
      const rowToken = String(keyParts[keyParts.length - 1] || "").trim();
      const rowMatch = rowToken.match(/row-(\d+)/i);
      return {
        rowNumber: rowMatch ? Number(rowMatch[1]) : null,
        companyCode: String(keyParts[0] || "").trim() || "-",
        clusterLabel: String(keyParts[1] || "").trim(),
        username: String(keyParts[2] || "").trim() || "-"
      };
    };

    const setLiveSummary = (rows) => {
      const list = Array.isArray(rows) ? rows : [];
      responseSummaryEl.innerHTML = "";
      if (list.length === 0) {
        const emptyText = document.createElement("div");
        emptyText.textContent = createJobRunning
          ? "İş başlatıldı. Firma bazlı satırlar alttaki tabloda hazırlanıyor."
          : "Firma bazlı canlı akış alttaki tabloda gösterilir.";
        responseSummaryEl.appendChild(emptyText);
        return;
      }

      const successCount = list.filter((event) => buildLiveEventTone(event) === "success").length;
      const errorCount = list.filter((event) => buildLiveEventTone(event) === "error").length;
      const pendingCount = Math.max(0, list.length - successCount - errorCount);

      const summaryText = document.createElement("div");
      summaryText.textContent = `Toplam ${list.length} firma/kullanıcı satırı izleniyor. Başarılı: ${successCount} | Hatalı: ${errorCount} | Bekleyen: ${pendingCount}`;
      responseSummaryEl.appendChild(summaryText);

      if (activeJobCreatedAt > 0) {
        const durationText = document.createElement("div");
        const effectiveFinishedAt =
          activeJobFinishedAt > 0 ? activeJobFinishedAt : createJobRunning ? Date.now() : 0;
        const durationMs = effectiveFinishedAt > 0 ? effectiveFinishedAt - activeJobCreatedAt : 0;
        durationText.textContent = `Çalışma süresi: ${formatLiveDuration(durationMs)}`;
        responseSummaryEl.appendChild(durationText);
      }

      const groupedMessages = new Map();
      list.forEach((event) => {
        const messageText = getLiveEventMessageText(event, { includePending: false });
        if (!messageText) return;
        const groupKey = normalizeLiveMessageText(messageText).toLocaleLowerCase("tr");
        const current = groupedMessages.get(groupKey) || {
          text: messageText,
          count: 0
        };
        current.count += 1;
        groupedMessages.set(groupKey, current);
      });

      if (groupedMessages.size > 0) {
        const groupList = document.createElement("div");
        groupList.className = "obus-user-create-message-group-list";

        Array.from(groupedMessages.values())
          .sort((a, b) => b.count - a.count || a.text.localeCompare(b.text, "tr"))
          .forEach((group) => {
            const item = document.createElement("div");
            item.className = `obus-user-create-message-group-item${
              isDuplicateUserMessage(group.text) ? " is-duplicate" : ""
            }`;

            const label = document.createElement("span");
            label.className = "obus-user-create-message-group-text";
            label.textContent = group.text;

            const count = document.createElement("span");
            count.className = "pill";
            count.textContent = `${group.count} adet`;

            item.appendChild(label);
            item.appendChild(count);
            groupList.appendChild(item);
          });

        responseSummaryEl.appendChild(groupList);
      }

      const hiddenDuplicateCount = list.filter((event) =>
        isDuplicateUserMessage(getLiveEventMessageText(event, { includePending: false }))
      ).length;
      if (hiddenDuplicateCount > 0) {
        const note = document.createElement("div");
        note.className = "obus-user-create-summary-note";
        note.textContent = `"Bu kullanıcı isimli kullanıcı daha önceden sisteme kayıt olmuştur." mesajlı ${hiddenDuplicateCount} satır tabloda gösterilmiyor.`;
        responseSummaryEl.appendChild(note);
      }
    };

    const appendLiveCell = (rowEl, label, value, extraClass = "") => {
      const cell = document.createElement("div");
      cell.className = "obus-user-create-live-cell";

      const cellLabel = document.createElement("span");
      cellLabel.className = "obus-user-create-live-cell-label";
      cellLabel.textContent = label;

      const cellValue = document.createElement("div");
      cellValue.className = `obus-user-create-live-cell-value${extraClass ? ` ${extraClass}` : ""}`;
      cellValue.textContent = value;

      cell.appendChild(cellLabel);
      cell.appendChild(cellValue);
      rowEl.appendChild(cell);
      return cellValue;
    };

    const renderJobResponseList = () => {
      responseListEl.innerHTML = "";
      const rows = buildLiveEventRows();
      setLiveSummary(rows);
      const visibleRows = rows.filter((event) => !isDuplicateUserMessage(getLiveEventMessageText(event, { includePending: false })));
      if (visibleRows.length === 0) {
        const emptyState = document.createElement("div");
        emptyState.className = "obus-user-create-live-empty";
        emptyState.textContent =
          rows.length > 0
            ? 'Tabloda gösterilecek farklı bir mesaj kalmadı. Tekrar kullanıcı kayıtları özet bölümünde sayılıyor.'
            : createJobRunning
              ? "Canlı durum hazırlanıyor. Firma bazlı satırlar birazdan burada görünecek."
              : "Toplu kullanıcı oluşturma henüz başlatılmadı.";
        responseListEl.appendChild(emptyState);
        return;
      }

      visibleRows.forEach((event) => {
        const tone = buildLiveEventTone(event);
        const meta = parseLiveEventMeta(event);
        const row = document.createElement("article");
        row.className = `obus-user-create-live-table-row is-${tone}`;

        appendLiveCell(row, "Satır", meta.rowNumber ? `Satır ${meta.rowNumber}` : "-", "is-muted");

        const companyCell = appendLiveCell(row, "Firma", meta.companyCode || "-");
        if (meta.clusterLabel) {
          companyCell.innerHTML = "";
          const stack = document.createElement("div");
          stack.className = "obus-user-create-live-cell-stack";
          const primary = document.createElement("strong");
          primary.textContent = meta.companyCode || "-";
          const secondary = document.createElement("span");
          secondary.className = "obus-user-create-live-cell-value is-muted";
          secondary.textContent = meta.clusterLabel;
          stack.appendChild(primary);
          stack.appendChild(secondary);
          companyCell.appendChild(stack);
        }

        appendLiveCell(row, "Kullanıcı", meta.username || "-");

        const statusCell = document.createElement("div");
        statusCell.className = "obus-user-create-live-cell";
        const statusLabel = document.createElement("span");
        statusLabel.className = "obus-user-create-live-cell-label";
        statusLabel.textContent = "Durum";
        const state = document.createElement("span");
        const stateToneClass = tone === "error" ? "danger" : tone === "success" ? "success" : "";
        state.className = `pill obus-user-create-live-row-state${stateToneClass ? ` ${stateToneClass}` : ""}`;
        state.textContent = buildLiveEventStateText(event);
        statusCell.appendChild(statusLabel);
        statusCell.appendChild(state);
        row.appendChild(statusCell);

        const messageText =
          getLiveEventMessageText(event, { includePending: true }) || (tone === "pending" ? "İstek akışı devam ediyor." : "");
        appendLiveCell(row, "Mesaj", messageText || "-", !messageText ? "is-muted" : "");

        const detailLines = Array.from(
          new Set(
            [
              String(event?.errorDetail || "").trim(),
              String(event?.detailText || "").trim(),
              ...(Array.isArray(event?.logLines) ? event.logLines : []).map((item) => String(item || "").trim())
            ].filter(Boolean)
          )
        );
        appendLiveCell(row, "Log Detayı", detailLines.join("\n") || "-", detailLines.length > 0 ? "is-log" : "is-muted");
        responseListEl.appendChild(row);
      });
    };

    const renderPreview = () => {
      const validation = validateEntriesForSubmit();
      const filledEntries = validation.ok ? validation.entries : readFilledEntries();
      const sampleEntry = filledEntries[0] || createEmptyEntry();
      const targetCount = companyCount > 0 ? companyCount * filledEntries.length : 0;

      previewEl.textContent = JSON.stringify(
        {
          "sample-request": buildSampleRequestBody(sampleEntry),
          meta: {
            templateId: currentTemplateId,
            templateName: normalizeTemplateName(templateNameInput.value || ""),
            requestReady: validation.ok && companyCount > 0,
            companyCount,
            entryCount: filledEntries.length,
            targetCount,
            samplePartnerId: samplePartnerId || 0,
            sampleBranchId: sampleBranchId || 0,
            validationError: validation.ok ? "" : validation.error
          }
        },
        null,
        2
      );
      if (!createJobRunning) renderJobResponseList();
      syncRowMeta();
    };

    const appendRow = (entry = createEmptyEntry(), { focus = false } = {}) => {
      const fragment = rowTemplate.content.cloneNode(true);
      const row = fragment.querySelector("[data-obus-user-create-row='1']");
      if (!row) return;

      const fullNameInput = row.querySelector("[data-obus-user-create-input='fullName']");
      const usernameInput = row.querySelector("[data-obus-user-create-input='username']");
      const passwordInput = row.querySelector("[data-obus-user-create-input='password']");

      if (fullNameInput) fullNameInput.value = String(entry.fullName || "");
      if (usernameInput) usernameInput.value = String(entry.username || "");
      if (passwordInput) passwordInput.value = String(entry.password || "");

      rowsContainer.appendChild(fragment);
      syncActionState();
      renderPreview();

      if (focus && fullNameInput) {
        window.requestAnimationFrame(() => {
          fullNameInput.focus();
          fullNameInput.select();
        });
      }
    };

    const renderRows = (entries) => {
      rowsContainer.innerHTML = "";
      const normalizedEntries = Array.isArray(entries)
        ? entries
            .map((entry) => ({
              fullName: String(entry?.fullName || ""),
              username: String(entry?.username || ""),
              password: String(entry?.password || "")
            }))
            .filter((entry) => entry.fullName || entry.username || entry.password)
        : [];
      const rowsToRender = normalizedEntries.length > 0 ? normalizedEntries : [createEmptyEntry()];
      rowsToRender.forEach((entry) => appendRow(entry));
      syncActionState();
      renderPreview();
    };

    const renderTemplateOptions = (selectedId = currentTemplateId) => {
      const normalizedSelectedId = String(selectedId || "").trim();
      templateSelect.innerHTML = "";

      const placeholderOption = document.createElement("option");
      placeholderOption.value = "";
      placeholderOption.textContent = templates.length > 0 ? "Şablon seçin" : "Kayıtlı şablon yok";
      templateSelect.appendChild(placeholderOption);

      const dateFormatter = new Intl.DateTimeFormat("tr-TR", {
        dateStyle: "short",
        timeStyle: "short"
      });

      templates.forEach((item) => {
        const option = document.createElement("option");
        option.value = String(item.id || "").trim();
        const updatedAtText = item.updatedAt ? dateFormatter.format(new Date(item.updatedAt)) : "-";
        option.textContent = `${item.name || "Adsız şablon"} (${Number(item.entryCount || 0)} kullanıcı) • ${updatedAtText}`;
        if (option.value === normalizedSelectedId) {
          option.selected = true;
        }
        templateSelect.appendChild(option);
      });

      syncActionState();
    };

    const buildDetailUrl = (templateId) => `${detailBaseUrl}/${encodeURIComponent(String(templateId || "").trim())}`;
    const buildDeleteUrl = (templateId) => `${deleteBaseUrl}/${encodeURIComponent(String(templateId || "").trim())}`;

    const stopActiveJob = () => {
      createJobRunning = false;
      activeJobId = "";
      activeJobCursor = 0;
      activeJobFailureCount = 0;
      if (activeJobPollTimerId) {
        window.clearTimeout(activeJobPollTimerId);
        activeJobPollTimerId = 0;
      }
      syncActionState();
    };

    const scheduleJobPoll = (delayMs = 900) => {
      if (!activeJobId) return;
      if (activeJobPollTimerId) {
        window.clearTimeout(activeJobPollTimerId);
      }
      activeJobPollTimerId = window.setTimeout(() => {
        void pollActiveJob();
      }, delayMs);
    };

    const pollActiveJob = async () => {
      if (!activeJobId) return;
      try {
        const response = await fetch(`/api/obus-live/${encodeURIComponent(activeJobId)}?cursor=${activeJobCursor}`, {
          headers: {
            Accept: "application/json"
          },
          cache: "no-store",
          credentials: "same-origin"
        });
        const data = await parseJsonResponse(response);
        if (!response.ok || !data?.ok) {
          throw new Error(getApiErrorMessage(response, data, "Toplu kullanıcı oluşturma durumu okunamadı"));
        }

        activeJobFailureCount = 0;
        activeJobCursor = Number.isFinite(Number(data.cursor)) ? Number(data.cursor) : activeJobCursor;
        activeJobCreatedAt = Number.isFinite(Number(data.createdAt)) ? Number(data.createdAt) : activeJobCreatedAt;
        activeJobFinishedAt = Number.isFinite(Number(data.finishedAt)) ? Number(data.finishedAt) : 0;
        if (Array.isArray(data.events) && data.events.length > 0) {
          activeJobEvents = activeJobEvents.concat(data.events);
        }

        renderJobResponseList();

        if (data.error) {
          setStatus(data.error, "error");
          stopActiveJob();
          return;
        }

        if (data.done) {
          const finalMessage =
            Number(data.failureCount || 0) > 0
              ? `Toplu kullanıcı oluşturma tamamlandı. Başarılı: ${Number(data.successCount || 0)} | Hatalı: ${Number(data.failureCount || 0)}`
              : `Toplu kullanıcı oluşturma tamamlandı. ${Number(data.successCount || 0)} istek başarılı.`;
          setStatus(finalMessage, Number(data.failureCount || 0) > 0 ? "error" : "success");
          stopActiveJob();
          return;
        }

        setStatus(
          `Toplu kullanıcı oluşturma sürüyor. İşlenen: ${Number(data.processedCount || 0)}/${Number(data.totalCount || 0)} | Başarılı: ${Number(data.successCount || 0)} | Hatalı: ${Number(data.failureCount || 0)}`,
          "muted"
        );
        scheduleJobPoll(900);
      } catch (err) {
        activeJobFailureCount += 1;
        if (activeJobFailureCount >= 3) {
          setStatus(err?.message || "Toplu kullanıcı oluşturma durumu okunamadı.", "error");
          stopActiveJob();
          return;
        }
        scheduleJobPoll(1400);
      }
    };

    const loadTemplates = async ({ selectedId = currentTemplateId, announceSuccess = false } = {}) => {
      setBusy(true);
      try {
        const response = await fetch(listUrl, {
          headers: {
            Accept: "application/json"
          }
        });
        const data = await parseJsonResponse(response);
        if (!response.ok || !data?.ok || !Array.isArray(data.items)) {
          throw new Error(getApiErrorMessage(response, data, "Şablon listesi okunamadı"));
        }

        templates = data.items || [];
        const selectedTemplate = templates.find((item) => String(item.id || "") === String(selectedId || ""));
        currentTemplateId = selectedTemplate ? selectedTemplate.id : null;
        renderTemplateOptions(selectedTemplate ? selectedTemplate.id : "");

        if (announceSuccess) {
          setStatus("Şablon listesi yenilendi.", "success");
        }
      } catch (err) {
        setStatus(err?.message || "Şablon listesi okunamadı.", "error");
      } finally {
        setBusy(false);
      }
    };

    const loadTemplateById = async (templateId) => {
      const normalizedId = String(templateId || "").trim();
      if (!normalizedId) {
        setStatus("Yüklenecek bir şablon seçin.", "error");
        return;
      }

      setBusy(true);
      try {
        const response = await fetch(buildDetailUrl(normalizedId), {
          headers: {
            Accept: "application/json"
          }
        });
        const data = await parseJsonResponse(response);
        if (!response.ok || !data?.ok || !data?.item) {
          throw new Error(getApiErrorMessage(response, data, "Şablon yüklenemedi"));
        }

        currentTemplateId = data.item.id;
        templateNameInput.value = String(data.item.name || "");
        renderRows(Array.isArray(data.item.entries) ? data.item.entries : []);
        renderTemplateOptions(currentTemplateId);
        setStatus(`${data.item.name || "Şablon"} yüklendi.`, "success");
      } catch (err) {
        setStatus(err?.message || "Şablon yüklenemedi.", "error");
      } finally {
        setBusy(false);
      }
    };

    const saveTemplate = async () => {
      const payload = {
        templateId: currentTemplateId,
        name: normalizeTemplateName(templateNameInput.value || ""),
        entries: readEntries()
      };

      if (!payload.name) {
        setStatus("Şablon adı zorunludur.", "error");
        templateNameInput.focus();
        return;
      }
      if (!readFilledEntries().length) {
        setStatus("Kaydetmek için en az bir kullanıcı satırı doldurun.", "error");
        return;
      }

      setBusy(true);
      try {
        const response = await fetch(saveUrl, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            Accept: "application/json"
          },
          body: JSON.stringify(payload)
        });
        const data = await parseJsonResponse(response);
        if (!response.ok || !data?.ok || !data?.item) {
          throw new Error(getApiErrorMessage(response, data, "Şablon kaydedilemedi"));
        }

        currentTemplateId = data.item.id;
        templateNameInput.value = String(data.item.name || payload.name || "");
        await loadTemplates({ selectedId: currentTemplateId });
        setStatus(data.action === "created" ? "Şablon kaydedildi." : "Şablon güncellendi.", "success");
      } catch (err) {
        setStatus(err?.message || "Şablon kaydedilemedi.", "error");
      } finally {
        setBusy(false);
      }
    };

    const deleteTemplate = async () => {
      const targetId = String(templateSelect.value || currentTemplateId || "").trim();
      if (!targetId) {
        setStatus("Silinecek bir şablon seçin.", "error");
        return;
      }

      const selectedTemplate = templates.find((item) => String(item.id || "") === targetId);
      if (!window.confirm(`${selectedTemplate?.name || "Seçili şablon"} silinsin mi?`)) {
        return;
      }

      setBusy(true);
      try {
        const response = await fetch(buildDeleteUrl(targetId), {
          method: "DELETE",
          headers: {
            Accept: "application/json"
          }
        });
        const data = await parseJsonResponse(response);
        if (!response.ok || data?.ok === false) {
          throw new Error(getApiErrorMessage(response, data, "Şablon silinemedi"));
        }

        if (String(currentTemplateId || "") === targetId) {
          currentTemplateId = null;
        }
        templateSelect.value = "";
        await loadTemplates({ selectedId: "" });
        setStatus("Şablon silindi.", "success");
      } catch (err) {
        setStatus(err?.message || "Şablon silinemedi.", "error");
      } finally {
        setBusy(false);
      }
    };

    rowsContainer.addEventListener("input", (event) => {
      const target = event.target;
      if (!(target instanceof HTMLInputElement)) return;
      if (!target.matches("[data-obus-user-create-input]")) return;
      renderPreview();
    });

    rowsContainer.addEventListener("click", (event) => {
      const button = event.target.closest("[data-obus-user-create-remove-row='1']");
      if (!button || isBusy()) return;
      const row = button.closest("[data-obus-user-create-row='1']");
      if (!row) return;

      if (getRows().length <= 1) {
        row.querySelectorAll("input").forEach((input) => {
          input.value = "";
        });
      } else {
        row.remove();
      }
      renderPreview();
    });

    addRowButton.addEventListener("click", () => {
      if (isBusy()) return;
      appendRow(createEmptyEntry(), { focus: true });
    });

    clearRowsButton.addEventListener("click", () => {
      if (isBusy()) return;
      renderRows([createEmptyEntry()]);
      setStatus("Satırlar temizlendi.", "success");
    });

    saveTemplateButton.addEventListener("click", () => {
      void saveTemplate();
    });

    refreshTemplatesButton.addEventListener("click", () => {
      void loadTemplates({ selectedId: currentTemplateId, announceSuccess: true });
    });

    loadTemplateButton.addEventListener("click", () => {
      void loadTemplateById(templateSelect.value);
    });

    deleteTemplateButton.addEventListener("click", () => {
      void deleteTemplate();
    });

    createUsersButton.addEventListener("click", () => {
      void (async () => {
        if (isBusy()) return;
        const validation = validateEntriesForSubmit();
        if (!validation.ok) {
          setStatus(validation.error, "error");
          renderPreview();
          return;
        }
        if (companyCount <= 0) {
          setStatus("Tüm Firmalar listesi boş. Önce firma listesini güncelleyin.", "error");
          return;
        }

        const targetCount = validation.entries.length * companyCount;
        const confirmed = window.confirm(
          `${validation.entries.length} kullanıcı satırı, ${companyCount} firmaya gönderilecek. Toplam ${targetCount} createuser isteği başlatılsın mı?`
        );
        if (!confirmed) return;

        setBusy(true);
        try {
          const response = await fetch(submitUrl, {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
              Accept: "application/json"
            },
            body: JSON.stringify({
              entries: validation.entries
            })
          });
          const data = await parseJsonResponse(response);
          if (!response.ok || !data?.ok || !data?.jobId) {
            throw new Error(getApiErrorMessage(response, data, "Toplu kullanıcı oluşturma başlatılamadı"));
          }

          createJobRunning = true;
          activeJobId = String(data.jobId || "").trim();
          activeJobCursor = 0;
          activeJobEvents = [];
          activeJobFailureCount = 0;
          activeJobCreatedAt = Date.now();
          activeJobFinishedAt = 0;
          syncActionState();
          renderJobResponseList();
          setStatus(
            `Toplu kullanıcı oluşturma başlatıldı. Hedef istek: ${Number(data.totalCount || targetCount)}`,
            "success"
          );
          scheduleJobPoll(300);
        } catch (err) {
          setStatus(err?.message || "Toplu kullanıcı oluşturma başlatılamadı.", "error");
        } finally {
          setBusy(false);
        }
      })();
    });

    templateSelect.addEventListener("change", () => {
      syncActionState();
    });

    templateNameInput.addEventListener("input", () => {
      renderPreview();
    });

    renderRows([createEmptyEntry()]);
    setStatus(
      companyCount > 0
        ? `Kullanıcı satırlarını hazırlayın. Girilen kullanıcılar ${companyCount} firmaya gönderilecek.`
        : "Tüm Firmalar listesi boş görünüyor. Önce firma listesini güncelleyin.",
      companyCount > 0 ? "muted" : "error"
    );
    void loadTemplates();
  };

  const initStationPassengerInfoPage = () => {
    const page = document.querySelector("[data-station-passenger-page='1']");
    if (!page) return;
    if (page.dataset.bound === "1") return;
    page.dataset.bound = "1";

    const stationPassengerSearchModeStorageKey = "stationPassengerSearchMode";
    const normalizeStationPassengerSearchMode = (value) =>
      String(value || "").trim().toLowerCase() === "all-stations" ? "all-stations" : "realtime";
    const plateInput = page.querySelector("[data-station-passenger-plate-input]");
    const goButton = page.querySelector("[data-station-passenger-go-button]");
    const modeToggleEl = page.querySelector("[data-station-passenger-mode-toggle]");
    const modeButtons = Array.from(page.querySelectorAll("[data-station-passenger-mode-button]"));
    const statusEl = page.querySelector("[data-station-passenger-status]");
    const resultsListEl = page.querySelector("[data-station-passenger-results-list]");
    const resultsCountEl = page.querySelector("[data-station-passenger-results-count]");
    const nextStopSectionEl = page.querySelector("[data-station-passenger-next-stop-section]");
    const nextStopCardEl = page.querySelector("[data-station-passenger-next-stop]");
    const nextStopOrderEl = page.querySelector("[data-station-passenger-next-stop-order]");
    const nextStopStationEl = page.querySelector("[data-station-passenger-next-stop-station]");
    const nextStopDepartureEl = page.querySelector("[data-station-passenger-next-stop-departure]");
    const passengerSectionEl = page.querySelector("[data-station-passenger-passenger-section]");
    const passengerStateCardEl = page.querySelector("[data-station-passenger-passenger-state]");
    const boardingListEl = page.querySelector("[data-station-passenger-boarding-list]");
    const boardingCountEl = page.querySelector("[data-station-passenger-boarding-count]");
    const dropoffListEl = page.querySelector("[data-station-passenger-dropoff-list]");
    const dropoffCountEl = page.querySelector("[data-station-passenger-dropoff-count]");
    if (
      !plateInput ||
      !goButton ||
      !modeToggleEl ||
      modeButtons.length < 2 ||
      !statusEl ||
      !resultsListEl ||
      !resultsCountEl ||
      !nextStopSectionEl ||
      !nextStopCardEl ||
      !nextStopOrderEl ||
      !nextStopStationEl ||
      !nextStopDepartureEl ||
      !passengerSectionEl ||
      !passengerStateCardEl ||
      !boardingListEl ||
      !boardingCountEl ||
      !dropoffListEl ||
      !dropoffCountEl
    ) {
      return;
    }

    let requestSequence = 0;
    let selectedResultIndex = -1;
    let selectedSearchMode = normalizeStationPassengerSearchMode(page.dataset.stationPassengerSearchMode);
    let resultItems = [];
    let selectedTripId = "";
    const journeyStationsCache = new Map();
    const journeyStationsRequestCache = new Map();
    const passengerStateCache = new Map();
    const passengerStateRequestCache = new Map();
    page.stationPassengerJourneyStationsCache = journeyStationsCache;
    page.stationPassengerPassengerStateCache = passengerStateCache;
    page.stationPassengerSelectedJourneyStations = [];
    page.stationPassengerSelectedNextStation = null;
    page.stationPassengerSelectedPassengerState = null;

    const readStoredSearchMode = () => {
      try {
        return normalizeStationPassengerSearchMode(window.localStorage.getItem(stationPassengerSearchModeStorageKey));
      } catch (err) {
        return "realtime";
      }
    };

    const persistSearchMode = (mode) => {
      try {
        window.localStorage.setItem(stationPassengerSearchModeStorageKey, mode);
      } catch (err) {
        // Ignore storage failures and keep in-memory mode.
      }
    };

    const setSearchMode = (mode, { persist = true } = {}) => {
      selectedSearchMode = normalizeStationPassengerSearchMode(mode);
      page.dataset.stationPassengerSearchMode = selectedSearchMode;
      modeToggleEl.dataset.searchMode = selectedSearchMode;

      modeButtons.forEach((buttonEl) => {
        const buttonMode = normalizeStationPassengerSearchMode(buttonEl.dataset.stationPassengerModeButton);
        const isActive = buttonMode === selectedSearchMode;
        buttonEl.classList.toggle("is-active", isActive);
        buttonEl.setAttribute("aria-pressed", isActive ? "true" : "false");
      });

      if (persist) {
        persistSearchMode(selectedSearchMode);
      }
    };

    setSearchMode(readStoredSearchMode(), { persist: false });

    modeButtons.forEach((buttonEl) => {
      buttonEl.addEventListener("click", () => {
        setSearchMode(buttonEl.dataset.stationPassengerModeButton);
      });
    });

    const normalizePlateDisplay = (value) =>
      String(value || "")
        .toLocaleUpperCase("tr")
        .replace(/[^0-9A-Z]/g, " ")
        .replace(/\s+/g, " ")
        .trim();

    const setStatus = (message, kind = "muted") => {
      statusEl.textContent = String(message || "").trim();
      if (kind === "error") {
        statusEl.className = "station-passenger-results-status alert inline-alert";
        return;
      }
      if (kind === "success") {
        statusEl.className = "station-passenger-results-status inline-success";
        return;
      }
      statusEl.className = "station-passenger-results-status muted";
    };

    const setCount = (count) => {
      const normalizedCount = Math.max(0, Number.parseInt(String(count || "0"), 10) || 0);
      resultsCountEl.textContent = String(normalizedCount);
      resultsCountEl.className = normalizedCount > 0 ? "pill" : "pill muted";
    };

    const setPassengerCount = (element, count) => {
      const normalizedCount = Math.max(0, Number.parseInt(String(count || "0"), 10) || 0);
      element.textContent = String(normalizedCount);
      element.className = normalizedCount > 0 ? "pill" : "pill muted";
    };

    const createPassengerListItem = (item) => {
      const rowEl = document.createElement("div");
      rowEl.className = "station-passenger-passenger-item";

      const titleEl = document.createElement("strong");
      titleEl.textContent = String(item?.passengerName || "").trim() || "Yolcu";

      const metaParts = [];
      const seatNumber = String(item?.seatNumber || "").trim();
      const ticketNumber = String(item?.ticketNumber || "").trim();
      if (seatNumber) {
        metaParts.push(`Koltuk ${seatNumber}`);
      }
      if (ticketNumber) {
        metaParts.push(`Bilet ${ticketNumber}`);
      }

      const metaEl = document.createElement("span");
      metaEl.textContent = metaParts.join(" • ").trim() || String(item?.label || "").trim() || "-";
      rowEl.appendChild(titleEl);
      rowEl.appendChild(metaEl);
      return rowEl;
    };

    const renderPassengerList = (containerEl, items, emptyMessage) => {
      containerEl.innerHTML = "";
      const normalizedItems = Array.isArray(items) ? items : [];
      if (normalizedItems.length === 0) {
        const emptyEl = document.createElement("div");
        emptyEl.className = "station-passenger-passenger-list-empty";
        emptyEl.textContent = String(emptyMessage || "").trim() || "Yolcu bulunamadı.";
        containerEl.appendChild(emptyEl);
        return;
      }

      const fragment = document.createDocumentFragment();
      normalizedItems.forEach((item) => {
        fragment.appendChild(createPassengerListItem(item));
      });
      containerEl.appendChild(fragment);
    };

    const scrollDetailsIntoView = (preferredSection = "next-stop") => {
      const targetEl =
        preferredSection === "passenger" && !passengerSectionEl.hidden
          ? passengerSectionEl
          : !nextStopSectionEl.hidden
            ? nextStopSectionEl
            : !passengerSectionEl.hidden
              ? passengerSectionEl
              : null;
      if (!targetEl || typeof targetEl.scrollIntoView !== "function") return;
      requestAnimationFrame(() => {
        targetEl.scrollIntoView({
          behavior: "smooth",
          block: "start",
          inline: "nearest"
        });
      });
    };

    const resetPassengerState = () => {
      passengerSectionEl.hidden = true;
      boardingListEl.innerHTML = "";
      dropoffListEl.innerHTML = "";
      setPassengerCount(boardingCountEl, 0);
      setPassengerCount(dropoffCountEl, 0);
      page.stationPassengerSelectedPassengerState = null;
    };

    const renderPassengerState = ({ stationId = "", boardingPassengers = [], dropoffPassengers = [] } = {}) => {
      const normalizedStationId = String(stationId || "").trim();
      const normalizedBoardingPassengers = Array.isArray(boardingPassengers) ? boardingPassengers : [];
      const normalizedDropoffPassengers = Array.isArray(dropoffPassengers) ? dropoffPassengers : [];
      if (!normalizedStationId) {
        resetPassengerState("Durak bilgisi bulunamadı.");
        return;
      }

      renderPassengerList(boardingListEl, normalizedBoardingPassengers, "Bu durakta binecek yolcu yok.");
      renderPassengerList(dropoffListEl, normalizedDropoffPassengers, "Bu durakta inecek yolcu yok.");
      setPassengerCount(boardingCountEl, normalizedBoardingPassengers.length);
      setPassengerCount(dropoffCountEl, normalizedDropoffPassengers.length);
      passengerSectionEl.hidden = false;
      passengerStateCardEl.hidden = false;
      page.stationPassengerSelectedPassengerState = {
        stationId: normalizedStationId,
        boardingPassengers: normalizedBoardingPassengers,
        dropoffPassengers: normalizedDropoffPassengers
      };
      scrollDetailsIntoView("passenger");
    };

    const buildPassengerStateCacheKey = (tripId, stationId) =>
      [String(tripId || "").trim(), String(stationId || "").trim()].join("|||").toLocaleLowerCase("tr");

    const getTurkishAblativeSuffix = (value) => {
      const normalized = String(value || "").trim().toLocaleLowerCase("tr");
      const vowels = normalized.match(/[aeiıioöuü]/g);
      const lastVowel = Array.isArray(vowels) && vowels.length > 0 ? vowels[vowels.length - 1] : "";
      if ("aıou".includes(lastVowel)) return "'dan";
      if ("eiöü".includes(lastVowel)) return "'den";
      return "'den";
    };

    const parseTimeToMinutes = (value) => {
      const text = String(value || "").trim();
      const match = text.match(/(\d{2}):(\d{2})/);
      if (!match) return null;
      const hour = Number.parseInt(match[1], 10);
      const minute = Number.parseInt(match[2], 10);
      if (!Number.isFinite(hour) || !Number.isFinite(minute)) return null;
      return hour * 60 + minute;
    };

    const findClosestUpcomingJourneyIndex = (items, requestDateTime) => {
      const normalizedItems = Array.isArray(items) ? items : [];
      const requestMinutes = parseTimeToMinutes(requestDateTime);
      if (!Number.isFinite(requestMinutes)) {
        return {
          requestMinutes: null,
          anchorIndex: -1
        };
      }

      const anchorIndex = normalizedItems.findIndex((item) => {
        const departureMinutes = parseTimeToMinutes(item?.departureTime);
        return Number.isFinite(departureMinutes) && departureMinutes >= requestMinutes;
      });

      return {
        requestMinutes,
        anchorIndex
      };
    };

    const renderNextStation = (nextStation, requestDate, fallbackMessage) => {
      const normalizedNextStation = nextStation && typeof nextStation === "object" ? nextStation : null;
      const stationId = String(
        normalizedNextStation?.stationId || normalizedNextStation?.["station-id"] || ""
      ).trim();
      const stationName = String(
        normalizedNextStation?.stationName || normalizedNextStation?.["station-name"] || ""
      ).trim();
      if (!normalizedNextStation || !stationId) {
        nextStopSectionEl.hidden = true;
        nextStopCardEl.hidden = true;
        nextStopOrderEl.textContent = "-";
        nextStopStationEl.textContent = "-";
        nextStopDepartureEl.textContent = "-";
        page.stationPassengerSelectedNextStation = null;
        return;
      }

      const orderValue = normalizedNextStation?.order;
      const orderText = Number.isFinite(Number(orderValue)) ? `#${Number(orderValue)}` : "-";
      const departureText = String(
        normalizedNextStation?.departureTime || normalizedNextStation?.["departure-time"] || ""
      ).trim();
      const stationDisplay = stationName || stationId;
      const departureLabel = `${stationDisplay}${getTurkishAblativeSuffix(stationDisplay)} Kalkış`;

      nextStopOrderEl.textContent = orderText;
      nextStopStationEl.textContent = `Varılacak Durak: ${stationDisplay}`;
      nextStopDepartureEl.textContent = departureText ? `${departureLabel}: ${departureText}` : `${departureLabel} saati yok`;
      nextStopSectionEl.hidden = false;
      nextStopCardEl.hidden = false;
      page.stationPassengerSelectedNextStation = normalizedNextStation;
      scrollDetailsIntoView("next-stop");
    };

    const setSelectedResultIndex = (index) => {
      selectedResultIndex = Number.isInteger(index) ? index : -1;
      resultsListEl.querySelectorAll("[data-station-passenger-result-button='1']").forEach((buttonEl) => {
        const buttonIndex = Number.parseInt(buttonEl.dataset.stationPassengerResultIndex || "-1", 10);
        buttonEl.classList.toggle("is-selected", buttonIndex === selectedResultIndex);
      });
      selectedTripId =
        selectedResultIndex >= 0 && resultItems[selectedResultIndex]
          ? String(resultItems[selectedResultIndex]?.tripId || "").trim()
          : "";
      page.dataset.stationPassengerSelectedTripId = selectedTripId;
      if (selectedTripId && journeyStationsCache.has(selectedTripId)) {
        const cachedResult = journeyStationsCache.get(selectedTripId);
        page.stationPassengerSelectedJourneyStations = Array.isArray(cachedResult?.items) ? cachedResult.items : [];
        renderNextStation(
          cachedResult?.nextStation,
          cachedResult?.requestDate,
          "İstek saatinden sonraki ilk durak bulunamadı."
        );
        const nextStationId = String(
          cachedResult?.nextStation?.stationId || cachedResult?.nextStation?.["station-id"] || ""
        ).trim();
        const passengerStateCacheKey = buildPassengerStateCacheKey(selectedTripId, nextStationId);
        if (nextStationId && passengerStateCache.has(passengerStateCacheKey)) {
          const cachedPassengerState = passengerStateCache.get(passengerStateCacheKey);
          renderPassengerState({
            stationId: nextStationId,
            boardingPassengers: cachedPassengerState?.boardingPassengers,
            dropoffPassengers: cachedPassengerState?.dropoffPassengers
          });
        } else {
          resetPassengerState();
        }
      } else {
        page.stationPassengerSelectedJourneyStations = [];
        renderNextStation(null, "", "");
        resetPassengerState();
      }
    };

    const normalizeResultItem = (item) => {
      const tripId =
        String(item?.tripId || item?.journeyId || item?.seferId || item?.id || "")
          .trim();
      return {
        ...item,
        id: tripId,
        tripId,
        journeyId: tripId,
        seferId: tripId
      };
    };

    const renderEmptyResults = (message) => {
      resultsListEl.innerHTML = "";
      const emptyEl = document.createElement("div");
      emptyEl.className = "station-passenger-results-empty";
      emptyEl.textContent = String(message || "").trim() || "Henüz listelenecek sefer yok.";
      resultsListEl.appendChild(emptyEl);
      resultItems = [];
      selectedTripId = "";
      journeyStationsCache.clear();
      journeyStationsRequestCache.clear();
      passengerStateCache.clear();
      passengerStateRequestCache.clear();
      page.dataset.stationPassengerSelectedTripId = "";
      page.stationPassengerSelectedJourneyStations = [];
      renderNextStation(null, "", "");
      resetPassengerState();
      setCount(0);
      setSelectedResultIndex(-1);
    };

    const renderResults = (items, requestDateTime = "") => {
      const normalizedItems = Array.isArray(items) ? items.map(normalizeResultItem) : [];
      journeyStationsCache.clear();
      journeyStationsRequestCache.clear();
      passengerStateCache.clear();
      passengerStateRequestCache.clear();
      page.stationPassengerSelectedJourneyStations = [];
      resultItems = normalizedItems;
      resultsListEl.innerHTML = "";

      if (normalizedItems.length === 0) {
        renderEmptyResults("Bu plakaya ait sefer bulunamadı.");
        return;
      }

      const journeyPosition = findClosestUpcomingJourneyIndex(normalizedItems, requestDateTime);
      const fragment = document.createDocumentFragment();
      normalizedItems.forEach((item, index) => {
        const rowButton = document.createElement("button");
        rowButton.type = "button";
        rowButton.className = "station-passenger-result-button";
        rowButton.dataset.stationPassengerResultButton = "1";
        rowButton.dataset.stationPassengerResultIndex = String(index);
        rowButton.dataset.stationPassengerTripId = String(item?.tripId || "").trim();

        const timeEl = document.createElement("span");
        timeEl.className = "station-passenger-result-time";
        timeEl.textContent = String(item?.departureTime || "").trim() || "-";
        const departureMinutes = parseTimeToMinutes(item?.departureTime);
        const isPastJourney =
          Number.isFinite(journeyPosition.requestMinutes) &&
          Number.isFinite(departureMinutes) &&
          departureMinutes < journeyPosition.requestMinutes;
        timeEl.classList.toggle("is-past", Boolean(isPastJourney));

        const routeWrapEl = document.createElement("span");
        routeWrapEl.className = "station-passenger-result-route";

        const routeLabelEl = document.createElement("span");
        routeLabelEl.className = "station-passenger-result-route-label";
        routeLabelEl.textContent = "Sefer Bilgisi";

        const routeValueEl = document.createElement("span");
        routeValueEl.className = "station-passenger-result-route-value";
        routeValueEl.textContent = String(item?.routeInfo || "").trim() || "-";

        routeWrapEl.appendChild(routeLabelEl);
        routeWrapEl.appendChild(routeValueEl);
        rowButton.appendChild(timeEl);
        rowButton.appendChild(routeWrapEl);
        fragment.appendChild(rowButton);
      });

      resultsListEl.appendChild(fragment);
      setCount(normalizedItems.length);
      setSelectedResultIndex(-1);

      if (Number.isInteger(journeyPosition.anchorIndex) && journeyPosition.anchorIndex >= 0) {
        setSelectedResultIndex(journeyPosition.anchorIndex);
        requestAnimationFrame(() => {
          const selectedButton = resultsListEl.querySelector(
            `[data-station-passenger-result-index='${journeyPosition.anchorIndex}']`
          );
          if (selectedButton && typeof selectedButton.scrollIntoView === "function") {
            selectedButton.scrollIntoView({
              block: "nearest",
              inline: "nearest"
            });
          }
        });
      }
    };

    const loadPassengerStateHistory = async ({ tripId, stationId, item, index }) => {
      const normalizedTripId = String(tripId || "").trim();
      const normalizedStationId = String(stationId || "").trim();
      if (!normalizedTripId || !normalizedStationId) {
        if (selectedTripId === normalizedTripId) {
          resetPassengerState();
        }
        window.dispatchEvent(
          new CustomEvent("station-passenger-passenger-state-error", {
            detail: {
              index,
              tripId: normalizedTripId,
              stationId: normalizedStationId,
              item,
              error: "journey-id ve station-id zorunludur."
            }
          })
        );
        return null;
      }

      const cacheKey = buildPassengerStateCacheKey(normalizedTripId, normalizedStationId);
      if (passengerStateCache.has(cacheKey)) {
        const cachedResponse = passengerStateCache.get(cacheKey);
        const selectedStationId = String(
          page.stationPassengerSelectedNextStation?.stationId ||
            page.stationPassengerSelectedNextStation?.["station-id"] ||
            ""
        ).trim();
        if (selectedTripId === normalizedTripId && selectedStationId === normalizedStationId) {
          renderPassengerState({
            stationId: normalizedStationId,
            boardingPassengers: cachedResponse?.boardingPassengers,
            dropoffPassengers: cachedResponse?.dropoffPassengers
          });
        }
        window.dispatchEvent(
          new CustomEvent("station-passenger-passenger-state-loaded", {
            detail: {
              index,
              tripId: normalizedTripId,
              stationId: normalizedStationId,
              item,
              response: cachedResponse,
              cached: true
            }
          })
        );
        return cachedResponse;
      }

      if (passengerStateRequestCache.has(cacheKey)) {
        return passengerStateRequestCache.get(cacheKey);
      }

      const requestPromise = (async () => {
        try {
          const response = await fetch("/api/station-passenger-info/passenger-state-history", {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
              Accept: "application/json"
            },
            body: JSON.stringify({
              tripId: normalizedTripId,
              stationId: normalizedStationId
            })
          });
          const data = await parseJsonResponse(response);
          if (!response.ok || data?.ok === false) {
            throw new Error(getApiErrorMessage(response, data, "GetPassengerStateHistory başarısız"));
          }

          const normalizedResponse = {
            ...data,
            stationId: normalizedStationId,
            boardingPassengers: Array.isArray(data?.boardingPassengers) ? data.boardingPassengers : [],
            dropoffPassengers: Array.isArray(data?.dropoffPassengers) ? data.dropoffPassengers : []
          };
          passengerStateCache.set(cacheKey, normalizedResponse);

          const selectedStationId = String(
            page.stationPassengerSelectedNextStation?.stationId ||
              page.stationPassengerSelectedNextStation?.["station-id"] ||
              ""
          ).trim();
          if (selectedTripId === normalizedTripId && selectedStationId === normalizedStationId) {
            renderPassengerState({
              stationId: normalizedStationId,
              boardingPassengers: normalizedResponse.boardingPassengers,
              dropoffPassengers: normalizedResponse.dropoffPassengers
            });
          }

          window.dispatchEvent(
            new CustomEvent("station-passenger-passenger-state-loaded", {
              detail: {
                index,
                tripId: normalizedTripId,
                stationId: normalizedStationId,
                item,
                response: normalizedResponse,
                cached: false
              }
            })
          );
          return normalizedResponse;
        } catch (err) {
          const selectedStationId = String(
            page.stationPassengerSelectedNextStation?.stationId ||
              page.stationPassengerSelectedNextStation?.["station-id"] ||
              ""
          ).trim();
          if (selectedTripId === normalizedTripId && selectedStationId === normalizedStationId) {
            resetPassengerState();
          }
          window.dispatchEvent(
            new CustomEvent("station-passenger-passenger-state-error", {
              detail: {
                index,
                tripId: normalizedTripId,
                stationId: normalizedStationId,
                item,
                error: String(err?.message || "GetPassengerStateHistory başarısız.").trim()
              }
            })
          );
          throw err;
        } finally {
          passengerStateRequestCache.delete(cacheKey);
        }
      })();

      passengerStateRequestCache.set(cacheKey, requestPromise);
      return requestPromise;
    };

    const loadJourneyStations = async ({ tripId, item, index }) => {
      const normalizedTripId = String(tripId || "").trim();
      if (!normalizedTripId) {
        window.dispatchEvent(
          new CustomEvent("station-passenger-journey-stations-error", {
            detail: {
              index,
              tripId: "",
              item,
              error: "Sefer id bulunamadı."
            }
          })
        );
        return null;
      }

      if (journeyStationsCache.has(normalizedTripId)) {
        const cachedResponse = journeyStationsCache.get(normalizedTripId);
        page.stationPassengerSelectedJourneyStations =
          selectedTripId === normalizedTripId && Array.isArray(cachedResponse?.items) ? cachedResponse.items : [];
        const cachedStationId = String(
          cachedResponse?.nextStation?.stationId || cachedResponse?.nextStation?.["station-id"] || ""
        ).trim();
        if (selectedTripId === normalizedTripId) {
          renderNextStation(
            cachedResponse?.nextStation,
            cachedResponse?.requestDate,
            "İstek saatinden sonraki ilk durak bulunamadı."
          );
          if (cachedStationId) {
            resetPassengerState();
            void loadPassengerStateHistory({
              tripId: normalizedTripId,
              stationId: cachedStationId,
              item,
              index
            }).catch(() => {});
          } else {
            resetPassengerState();
          }
        }
        window.dispatchEvent(
          new CustomEvent("station-passenger-journey-stations-loaded", {
            detail: {
              index,
              tripId: normalizedTripId,
              item,
              items: Array.isArray(cachedResponse?.items) ? cachedResponse.items : [],
              response: cachedResponse,
              cached: true
            }
          })
        );
        return cachedResponse;
      }

      if (journeyStationsRequestCache.has(normalizedTripId)) {
        return journeyStationsRequestCache.get(normalizedTripId);
      }

      const requestPromise = (async () => {
        try {
          const response = await fetch("/api/station-passenger-info/journey-stations", {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
              Accept: "application/json"
            },
            body: JSON.stringify({
              tripId: normalizedTripId
            })
          });
          const data = await parseJsonResponse(response);
          if (!response.ok || data?.ok === false) {
            throw new Error(getApiErrorMessage(response, data, "GetJourneyStations başarısız"));
          }

          const normalizedItems = Array.isArray(data?.items) ? data.items : [];
          const normalizedResponse = {
            ...data,
            items: normalizedItems
          };
          if (resultItems[index]) {
            resultItems[index].journeyStations = normalizedItems;
          }
          journeyStationsCache.set(normalizedTripId, normalizedResponse);
          const nextStationId = String(
            normalizedResponse?.nextStation?.stationId || normalizedResponse?.nextStation?.["station-id"] || ""
          ).trim();
          if (selectedTripId === normalizedTripId) {
            page.stationPassengerSelectedJourneyStations = normalizedItems;
            renderNextStation(
              normalizedResponse?.nextStation,
              normalizedResponse?.requestDate,
              "İstek saatinden sonraki ilk durak bulunamadı."
            );
            if (nextStationId) {
              resetPassengerState();
              void loadPassengerStateHistory({
                tripId: normalizedTripId,
                stationId: nextStationId,
                item,
                index
              }).catch(() => {});
            } else {
              resetPassengerState();
            }
          }
          window.dispatchEvent(
            new CustomEvent("station-passenger-journey-stations-loaded", {
              detail: {
                index,
                tripId: normalizedTripId,
                item,
                items: normalizedItems,
                response: normalizedResponse,
                cached: false
              }
            })
          );
          return normalizedResponse;
        } catch (err) {
          if (selectedTripId === normalizedTripId) {
            renderNextStation(null, "", "İlk durak bilgisi alınamadı.");
            resetPassengerState();
          }
          window.dispatchEvent(
            new CustomEvent("station-passenger-journey-stations-error", {
              detail: {
                index,
                tripId: normalizedTripId,
                item,
                error: String(err?.message || "GetJourneyStations başarısız.").trim()
              }
            })
          );
          throw err;
        } finally {
          journeyStationsRequestCache.delete(normalizedTripId);
        }
      })();

      journeyStationsRequestCache.set(normalizedTripId, requestPromise);
      return requestPromise;
    };

    const executeSearchRequest = async () => {
      const displayPlate = normalizePlateDisplay(plateInput.value);
      if (!displayPlate.replace(/\s+/g, "")) {
        setStatus("Plaka girilmesi zorunludur.", "error");
        renderEmptyResults("Arama yapmak için plaka girin.");
        return;
      }

      plateInput.value = displayPlate;
      const currentRequest = ++requestSequence;
      goButton.disabled = true;
      setStatus("Sefer aranıyor...", "muted");
      renderEmptyResults("Sefer aranıyor...");

      try {
        const response = await fetch("/api/station-passenger-info/search", {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            Accept: "application/json"
          },
          body: JSON.stringify({
            plate: displayPlate
          })
        });
        const data = await parseJsonResponse(response);
        if (currentRequest !== requestSequence) return;

        if (!response.ok || data?.ok === false) {
          const message = getApiErrorMessage(response, data, "Plaka araması başarısız");
          setStatus(message, "error");
          renderEmptyResults("Bu plakaya ait sefer bulunamadı.");
          return;
        }

        const items = Array.isArray(data?.items) ? data.items : [];
        renderResults(items, data?.requestDateTime || "");

        if (items.length === 0) {
          const searchedPlate = normalizePlateDisplay(data?.searchedPlate || displayPlate) || displayPlate;
          setStatus(`${searchedPlate} için sefer bulunamadı.`, "muted");
          return;
        }

        setStatus(`${items.length} sefer bulundu.`, "success");
      } catch (err) {
        if (currentRequest !== requestSequence) return;
        setStatus(err?.message || "Plaka araması başarısız.", "error");
        renderEmptyResults("Arama sırasında hata oluştu.");
      } finally {
        if (currentRequest === requestSequence) {
          goButton.disabled = false;
        }
      }
    };

    const runRealtimeSearch = async () => executeSearchRequest();
    const runAllStationsSearch = async () => executeSearchRequest();

    const runSearch = async () => {
      if (selectedSearchMode === "all-stations") {
        return runAllStationsSearch();
      }
      return runRealtimeSearch();
    };

    const handleRealtimeJourneySelection = async ({ tripId, item, index }) =>
      loadJourneyStations({ tripId, item, index });
    const handleAllStationsJourneySelection = async ({ tripId, item, index }) =>
      loadJourneyStations({ tripId, item, index });

    const handleJourneySelection = async ({ tripId, item, index }) => {
      if (selectedSearchMode === "all-stations") {
        return handleAllStationsJourneySelection({ tripId, item, index });
      }
      return handleRealtimeJourneySelection({ tripId, item, index });
    };

    plateInput.addEventListener("input", () => {
      const normalized = String(plateInput.value || "")
        .toLocaleUpperCase("tr")
        .replace(/[^0-9A-Z\s]/g, " ")
        .replace(/\s{2,}/g, " ");
      if (plateInput.value !== normalized) {
        plateInput.value = normalized;
      }
    });

    plateInput.addEventListener("keydown", (event) => {
      if (event.key !== "Enter") return;
      event.preventDefault();
      runSearch();
    });

    goButton.addEventListener("click", () => {
      runSearch();
    });

    resultsListEl.addEventListener("click", (event) => {
      const resultButton = event.target.closest("[data-station-passenger-result-button='1']");
      if (!resultButton) return;
      const nextIndex = Number.parseInt(resultButton.dataset.stationPassengerResultIndex || "-1", 10);
      if (!Number.isInteger(nextIndex) || !resultItems[nextIndex]) return;
      setSelectedResultIndex(nextIndex);
      window.dispatchEvent(
        new CustomEvent("station-passenger-result-selected", {
          detail: {
            index: nextIndex,
            tripId: String(resultItems[nextIndex]?.tripId || "").trim(),
            item: resultItems[nextIndex]
          }
        })
      );
      void handleJourneySelection({
        tripId: String(resultItems[nextIndex]?.tripId || "").trim(),
        item: resultItems[nextIndex],
        index: nextIndex
      }).catch(() => {});
    });
  };

  const initAllCompaniesLoading = () => {
    const forms = Array.from(document.querySelectorAll(".all-companies-loading-form"));
    if (forms.length === 0) return;

    forms.forEach((form) => {
      if (form.dataset.loadingBound === "1") return;
      form.dataset.loadingBound = "1";

      const submitBtn = form.querySelector("button[type='submit']");
      const loadingMessage = form.querySelector(".all-companies-loading-message");
      if (!submitBtn) return;

      const defaultLabel = String(submitBtn.dataset.defaultLabel || submitBtn.textContent || "").trim() || "Yenile";
      const loadingLabel = String(submitBtn.dataset.loadingLabel || "").trim() || "Yükleniyor...";

      form.classList.remove("is-loading");
      submitBtn.disabled = false;
      submitBtn.textContent = defaultLabel;
      if (loadingMessage) {
        loadingMessage.hidden = true;
      }

      form.addEventListener("submit", () => {
        submitBtn.disabled = true;
        submitBtn.textContent = loadingLabel;
        form.classList.add("is-loading");
        if (loadingMessage) {
          loadingMessage.hidden = false;
        }
      });
    });
  };

  const initAllCompaniesObusJobMonitor = () => {
    const statusBoxes = Array.from(document.querySelectorAll("[data-all-companies-job='1']"));
    if (statusBoxes.length === 0) return;

    const formatElapsedTime = (elapsedMs) => {
      const totalSeconds = Math.max(0, Math.floor(Number(elapsedMs || 0) / 1000));
      const hours = Math.floor(totalSeconds / 3600);
      const minutes = Math.floor((totalSeconds % 3600) / 60);
      const seconds = totalSeconds % 60;
      if (hours > 0) {
        return `${String(hours).padStart(2, "0")}:${String(minutes).padStart(2, "0")}:${String(seconds).padStart(2, "0")}`;
      }
      return `${String(minutes).padStart(2, "0")}:${String(seconds).padStart(2, "0")}`;
    };

    const renderStatusBox = (statusBox, message, options = {}) => {
      const busy = options.busy === true;
      const showCounts = options.showCounts === true;
      const processed = Number(options.processed || 0);
      const total = Number(options.total || 0);
      const success = Number(options.success || 0);
      const failure = Number(options.failure || 0);
      const elapsedMs = Number(options.elapsedMs || 0);

      statusBox.textContent = "";
      statusBox.classList.toggle("all-companies-job-active", busy);

      if (busy) {
        const spinner = document.createElement("span");
        spinner.className = "all-companies-job-spinner";
        spinner.setAttribute("aria-hidden", "true");
        statusBox.appendChild(spinner);
      }

      const text = document.createElement("span");
      text.className = "all-companies-job-text";
      text.textContent = showCounts
        ? `${message} İşlenen: ${processed}/${total} | Başarılı: ${success} | Hatalı: ${failure}`
        : message;
      statusBox.appendChild(text);

      if (busy) {
        const timer = document.createElement("span");
        timer.className = "all-companies-job-timer";
        timer.textContent = formatElapsedTime(elapsedMs);
        statusBox.appendChild(timer);
      }
    };

    statusBoxes.forEach((statusBox) => {
      if (statusBox.dataset.jobMonitorBound === "1") return;
      statusBox.dataset.jobMonitorBound = "1";

      const jobId = String(statusBox.getAttribute("data-job-id") || "").trim();
      const jobDone = String(statusBox.getAttribute("data-job-done") || "").trim() === "1";
      const jobCreatedAtRaw = Number.parseInt(String(statusBox.getAttribute("data-job-created-at") || "0"), 10);
      const jobCreatedAt = Number.isFinite(jobCreatedAtRaw) && jobCreatedAtRaw > 0 ? jobCreatedAtRaw : Date.now();
      const refreshUrl =
        String(statusBox.getAttribute("data-refresh-url") || "").trim() || window.location.pathname + window.location.search;
      const runningMessage = String(statusBox.getAttribute("data-running-message") || "").trim();
      const showCounts = String(statusBox.getAttribute("data-show-counts") || "").trim() === "1";
      if (!jobId || jobDone) return;

      let cursor = 0;
      let transientFailureCount = 0;
      let latestState = {
        processed: 0,
        total: 0,
        success: 0,
        failure: 0
      };

      renderStatusBox(statusBox, runningMessage || "İşlem sürüyor. Sayfa otomatik yenilenecek...", {
        busy: true,
        showCounts,
        processed: latestState.processed,
        total: latestState.total,
        success: latestState.success,
        failure: latestState.failure,
        elapsedMs: Date.now() - jobCreatedAt
      });

      const timerId = window.setInterval(() => {
        renderStatusBox(statusBox, runningMessage || "İşlem sürüyor. Sayfa otomatik yenilenecek...", {
          busy: true,
          showCounts,
          processed: latestState.processed,
          total: latestState.total,
          success: latestState.success,
          failure: latestState.failure,
          elapsedMs: Date.now() - jobCreatedAt
        });
      }, 1000);

      const poll = async () => {
        try {
          const response = await fetch(`/api/obus-live/${encodeURIComponent(jobId)}?cursor=${cursor}`, {
            headers: {
              Accept: "application/json"
            },
            cache: "no-store",
            credentials: "same-origin"
          });
          const data = await parseJsonResponse(response);
          const isLoginRedirect = response.status === 401 || (response.redirected && /\/login(?:$|[?#])/.test(response.url || ""));
          const isTransientStatus = [502, 503, 504].includes(Number(response.status || 0));
          if (isTransientStatus) {
            throw new Error("__TRANSIENT__");
          }
          if (isLoginRedirect) {
            throw new Error("__TRANSIENT__");
          }
          if (!response.ok || !data?.ok) {
            throw new Error(getApiErrorMessage(response, data, "İş durumu alınamadı"));
          }

          transientFailureCount = 0;
          cursor = Number.isFinite(Number(data.cursor)) ? Number(data.cursor) : cursor;
          latestState = {
            processed: Number(data.processedCount || 0),
            total: Number(data.totalCount || 0),
            success: Number(data.successCount || 0),
            failure: Number(data.failureCount || 0)
          };
          renderStatusBox(statusBox, runningMessage || "İşlem sürüyor. Sayfa otomatik yenilenecek...", {
            busy: true,
            showCounts,
            processed: latestState.processed,
            total: latestState.total,
            success: latestState.success,
            failure: latestState.failure,
            elapsedMs: Date.now() - jobCreatedAt
          });

          if (data.done) {
            window.clearInterval(timerId);
            navigate(refreshUrl, { push: true });
            return;
          }
        } catch (err) {
          const errorCode = String(err?.message || "").trim();
          if (errorCode === "__TRANSIENT__" || !errorCode || /Failed to fetch/i.test(errorCode)) {
            transientFailureCount += 1;
            renderStatusBox(statusBox, runningMessage || "İşlem sürüyor. Sayfa otomatik yenilenecek...", {
              busy: true,
              showCounts,
              processed: latestState.processed,
              total: latestState.total,
              success: latestState.success,
              failure: latestState.failure,
              elapsedMs: Date.now() - jobCreatedAt
            });
            window.setTimeout(poll, transientFailureCount >= 3 ? 5000 : 2500);
            return;
          }

          window.clearInterval(timerId);
          renderStatusBox(statusBox, err?.message || "İş durumu okunamadı.", {
            busy: false
          });
          return;
        }

        window.setTimeout(poll, 2500);
      };

      window.setTimeout(poll, 1000);
    });
  };

  const loadMentiChatGptChatStateFromStorage = () => {
    try {
      const raw = window.localStorage.getItem(mentiChatGptChatStorageKey);
      if (!raw) return { draft: "", messages: [] };
      const parsed = JSON.parse(raw);
      const messages = Array.isArray(parsed?.messages)
        ? parsed.messages
            .map((item) => ({
              role: String(item?.role || "").trim().toLowerCase() === "assistant" ? "assistant" : "user",
              text: String(item?.text || "").trim()
            }))
            .filter((item) => item.text)
            .slice(-20)
        : [];
      return {
        draft: String(parsed?.draft || ""),
        messages
      };
    } catch (err) {
      return { draft: "", messages: [] };
    }
  };

  const saveMentiChatGptChatStateToStorage = (state) => {
    try {
      window.localStorage.setItem(mentiChatGptChatStorageKey, JSON.stringify(state));
      return true;
    } catch (err) {
      return false;
    }
  };

  const initMentiHelper = () => {
    if (typeof mentiChatGptChatRuntime.cleanup === "function") {
      mentiChatGptChatRuntime.cleanup();
      mentiChatGptChatRuntime.cleanup = null;
    }

    const root = document.querySelector("[data-menti-chatgpt-chat='1']");
    if (!root) return;
    const chatGptUrl = String(root.getAttribute("data-menti-chatgpt-url") || "https://chatgpt.com/").trim();

    const form = root.querySelector("[data-menti-chatgpt-form='1']");
    const inputEl = root.querySelector("[data-menti-chatgpt-input='1']");
    const sendBtn = root.querySelector("[data-menti-chatgpt-send='1']");
    const openBtn = root.querySelector("[data-menti-chatgpt-open='1']");
    const clearBtn = root.querySelector("[data-menti-chatgpt-clear='1']");
    const messagesEl = root.querySelector("[data-menti-chatgpt-messages='1']");
    const emptyEl = root.querySelector("[data-menti-chatgpt-empty='1']");
    const statusEl = root.querySelector("[data-menti-chatgpt-status='1']");

    if (!form || !inputEl || !sendBtn || !openBtn || !clearBtn || !messagesEl || !emptyEl || !statusEl) {
      return;
    }

    const state = loadMentiChatGptChatStateFromStorage();
    let messages = Array.isArray(state.messages) ? state.messages.slice(-20) : [];
    let isActive = true;

    const persistState = () => {
      if (!isActive) return;
      saveMentiChatGptChatStateToStorage({
        draft: String(inputEl.value || ""),
        messages
      });
    };

    const setStatus = (message, isError = false) => {
      if (!isActive) return;
      statusEl.textContent = message;
      statusEl.classList.toggle("is-error", Boolean(isError));
    };

    const syncButtons = () => {
      if (!isActive) return;
      const hasDraft = Boolean(String(inputEl.value || "").trim());
      sendBtn.disabled = !hasDraft;
      openBtn.disabled = false;
      clearBtn.disabled = !messages.length && !hasDraft;
    };

    const renderMessages = () => {
      if (!isActive) return;
      const messageNodes = Array.from(messagesEl.querySelectorAll("[data-menti-chatgpt-message='1']"));
      messageNodes.forEach((node) => node.remove());
      emptyEl.hidden = messages.length > 0;

      messages.forEach((message) => {
        const article = document.createElement("article");
        article.setAttribute("data-menti-chatgpt-message", "1");
        article.className = `menti-chatgpt-message ${message.role === "assistant" ? "assistant" : "user"}`;

        const meta = document.createElement("div");
        meta.className = "menti-chatgpt-message-meta";
        meta.textContent = message.role === "assistant" ? "Not" : "Taslak";

        const body = document.createElement("div");
        body.className = "menti-chatgpt-message-body";
        body.textContent = message.text;

        article.appendChild(meta);
        article.appendChild(body);
        messagesEl.appendChild(article);
      });

      messagesEl.scrollTop = messagesEl.scrollHeight;
    };

    const appendMessage = (role, text) => {
      const normalizedText = String(text || "").trim();
      if (!normalizedText) return;
      messages.push({ role, text: normalizedText });
      messages = messages.slice(-20);
      renderMessages();
      persistState();
      syncButtons();
    };

    const addNote = (text) => {
      appendMessage("assistant", text);
    };

    const clearChat = () => {
      messages = [];
      inputEl.value = "";
      renderMessages();
      persistState();
      syncButtons();
      setStatus("Taslak gecmisi temizlendi.");
    };

    const copyDraftToClipboard = async () => {
      const prompt = String(inputEl.value || "").trim();
      if (!prompt) return false;

      try {
        if (!navigator.clipboard || typeof navigator.clipboard.writeText !== "function") {
          throw new Error("Tarayici panoya kopyalama izni vermedi.");
        }
        await navigator.clipboard.writeText(prompt);
        appendMessage("user", prompt);
        addNote("Mesaj panoya kopyalandi. ChatGPT sekmesini acip yapistirabilirsiniz.");
        inputEl.value = "";
        persistState();
        syncButtons();
        setStatus("Mesaj panoya kopyalandi.");
        return true;
      } catch (err) {
        if (!isActive) return;
        setStatus(err?.message || "Mesaj kopyalanamadi.", true);
        return false;
      }
    };

    const openChatGpt = () => {
      if (!isActive) return;
      const opened = window.open(chatGptUrl || "https://chatgpt.com/", "_blank", "noopener,noreferrer");
      if (!opened) {
        setStatus("ChatGPT sekmesi acilamadi. Popup engelleyiciyi kontrol edin.", true);
        return;
      }
      setStatus("ChatGPT yeni sekmede acildi.");
    };

    inputEl.value = state.draft || "";
    renderMessages();
    syncButtons();
    setStatus(
      messages.length
        ? "Kaydedilen taslaklar yuklendi."
        : "Mesajinizi yazin, Kopyala'ya basin, sonra ChatGPT sekmesinde yapistirin."
    );

    const handleSubmit = async (event) => {
      event.preventDefault();
      await copyDraftToClipboard();
    };
    const handleClear = () => {
      clearChat();
    };
    const handleOpen = async () => {
      if (String(inputEl.value || "").trim()) {
        const copied = await copyDraftToClipboard();
        if (!copied) return;
      }
      openChatGpt();
    };
    const handleInput = () => {
      persistState();
      syncButtons();
    };
    const handleKeydown = (event) => {
      if (event.key === "Enter" && !event.shiftKey) {
        event.preventDefault();
        void copyDraftToClipboard();
      }
    };

    form.addEventListener("submit", handleSubmit);
    openBtn.addEventListener("click", handleOpen);
    clearBtn.addEventListener("click", handleClear);
    inputEl.addEventListener("input", handleInput);
    inputEl.addEventListener("keydown", handleKeydown);

    mentiChatGptChatRuntime.cleanup = () => {
      isActive = false;
      form.removeEventListener("submit", handleSubmit);
      openBtn.removeEventListener("click", handleOpen);
      clearBtn.removeEventListener("click", handleClear);
      inputEl.removeEventListener("input", handleInput);
      inputEl.removeEventListener("keydown", handleKeydown);
    };
  };

  const initPermissionsBulkForm = () => {
    const form = document.querySelector("form[action^='/permissions/']");
    if (!form) return;
    if (form.dataset.permissionsBound === "1") return;
    form.dataset.permissionsBound = "1";

    const touchedInput = form.querySelector("input[name='sectionTouchedJson']");
    const touchedSections = new Set();
    const syncLogCheckboxState = (itemCheckbox) => {
      if (!(itemCheckbox instanceof HTMLInputElement)) return;
      const row = itemCheckbox.closest(".permission-check-row");
      const logCheckbox = row?.querySelector("input[name='menuLogKeys']");
      if (!(logCheckbox instanceof HTMLInputElement)) return;
      if (itemCheckbox.disabled) {
        logCheckbox.disabled = true;
        return;
      }
      logCheckbox.disabled = !itemCheckbox.checked;
      if (!itemCheckbox.checked) {
        logCheckbox.checked = false;
      }
    };
    const syncTouchedInput = () => {
      if (!touchedInput) return;
      touchedInput.value = JSON.stringify(Array.from(touchedSections));
    };

    const sectionToggles = Array.from(form.querySelectorAll("[data-section-toggle]"));
    sectionToggles.forEach((toggle) => {
      toggle.addEventListener("change", () => {
        const sectionKey = String(toggle.getAttribute("data-section-toggle") || "").trim();
        if (!sectionKey) return;
        touchedSections.add(sectionKey);
        syncTouchedInput();
        form
          .querySelectorAll(`[data-parent-section="${sectionKey}"] [data-item-checkbox="1"]`)
          .forEach((checkbox) => {
            if (checkbox.disabled) return;
            checkbox.checked = toggle.checked;
            syncLogCheckboxState(checkbox);
          });
      });
    });

    const itemCheckboxes = Array.from(form.querySelectorAll("[data-item-checkbox='1']"));
    itemCheckboxes.forEach((checkbox) => {
      syncLogCheckboxState(checkbox);
      checkbox.addEventListener("change", () => {
        const sectionKey = String(checkbox.getAttribute("data-parent-section") || "").trim();
        syncLogCheckboxState(checkbox);
        if (!sectionKey) return;
        const sectionToggle = form.querySelector(`[data-section-toggle="${sectionKey}"]`);
        if (!sectionToggle || sectionToggle.disabled) return;
        if (checkbox.checked) {
          sectionToggle.checked = true;
          return;
        }
        const sectionItems = Array.from(
          form.querySelectorAll(`[data-parent-section="${sectionKey}"] [data-item-checkbox="1"]`)
        );
        sectionToggle.checked = sectionItems.some((item) => item.checked);
      });
    });
  };

  const initScreenLogPanel = () => {
    const panels = Array.from(document.querySelectorAll("[data-screen-log-panel='1']"));
    if (panels.length === 0) return;

    const syncScreenLogBodyState = () => {
      const hasOpenModal = Array.from(document.querySelectorAll("[data-screen-log-modal='1']")).some(
        (item) => item instanceof HTMLElement && !item.hidden
      );
      document.body.classList.toggle("screen-log-modal-open", hasOpenModal);
    };

    panels.forEach((panel) => {
      if (panel.dataset.screenLogBound === "1") return;
      panel.dataset.screenLogBound = "1";

      const toggleBtn = panel.querySelector("[data-screen-log-toggle='1']");
      const refreshBtn = panel.querySelector("[data-screen-log-refresh='1']");
      const modal = panel.querySelector("[data-screen-log-modal='1']");
      const dialog = panel.querySelector("[data-screen-log-dialog='1']");
      const stateEl = panel.querySelector("[data-screen-log-state='1']");
      const listEl = panel.querySelector("[data-screen-log-list='1']");
      const closeButtons = Array.from(panel.querySelectorAll("[data-screen-log-close='1']"));
      const apiPath = String(panel.getAttribute("data-screen-log-api-path") || "").trim();

      if (!toggleBtn || !modal || !dialog || !listEl || !apiPath) return;

      const escapeHtml = (value) =>
        String(value || "")
          .replace(/&/g, "&amp;")
          .replace(/</g, "&lt;")
          .replace(/>/g, "&gt;")
          .replace(/\"/g, "&quot;")
          .replace(/'/g, "&#39;");

      const setOpen = (open) => {
        modal.hidden = !open;
        modal.setAttribute("aria-hidden", open ? "false" : "true");
        toggleBtn.setAttribute("aria-expanded", open ? "true" : "false");
        syncScreenLogBodyState();
        if (open) {
          window.requestAnimationFrame(() => {
            dialog.focus();
          });
        }
      };

      const closeModal = () => {
        setOpen(false);
        window.requestAnimationFrame(() => {
          toggleBtn.focus();
        });
      };

      const setState = (text = "", kind = "") => {
        if (!stateEl) return;
        stateEl.textContent = String(text || "").trim();
        stateEl.className = `screen-log-state ${kind === "error" ? "inline-alert" : "muted"}`.trim();
      };

      const renderItems = (items) => {
        const rows = Array.isArray(items) ? items : [];
        if (rows.length === 0) {
          listEl.innerHTML = '<div class="screen-log-empty">Henüz ekran logu yok.</div>';
          return;
        }

        listEl.innerHTML = rows
          .map((item) => {
            const level = escapeHtml(String(item?.level || "info").trim() || "info");
            const createdAt = escapeHtml(String(item?.createdAt || "").trim());
            const message = escapeHtml(String(item?.message || "").trim());
            const method = escapeHtml(String(item?.requestMethod || "").trim());
            const path = escapeHtml(String(item?.requestPath || "").trim());
            const createdByName = escapeHtml(String(item?.createdByName || "-").trim() || "-");
            const detailText = escapeHtml(String(item?.detailText || "").trim());
            return `
              <article class="screen-log-item screen-log-item-${level}">
                <div class="screen-log-item-head">
                  <span class="screen-log-item-level">${level.toUpperCase()}</span>
                  <time datetime="${createdAt}">${createdAt || "-"}</time>
                </div>
                <strong class="screen-log-item-message">${message || "-"}</strong>
                <div class="screen-log-item-meta">
                  <span>${method || "-"}</span>
                  <span>${path || "-"}</span>
                  <span>${createdByName}</span>
                </div>
                ${detailText ? `<pre class="screen-log-item-detail">${detailText}</pre>` : ""}
              </article>
            `;
          })
          .join("");
      };

      const refreshLogs = async () => {
        if (panel.dataset.screenLogLoading === "1") return;
        panel.dataset.screenLogLoading = "1";
        setState("Loglar yükleniyor...");
        try {
          const response = await fetch(apiPath, {
            headers: { Accept: "application/json" }
          });
          const data = await parseJsonResponse(response);
          if (!response.ok || !data?.ok) {
            throw new Error(getApiErrorMessage(response, data, "Ekran logları okunamadı"));
          }
          renderItems(data.items || []);
          setState("Son 20 kayıt gösteriliyor.");
        } catch (err) {
          setState(err?.message || "Ekran logları okunamadı.", "error");
        } finally {
          panel.dataset.screenLogLoading = "0";
        }
      };

      toggleBtn.addEventListener("click", (event) => {
        event.preventDefault();
        event.stopPropagation();
        const nextOpen = modal.hidden;
        setOpen(nextOpen);
        if (nextOpen) {
          void refreshLogs();
        }
      });

      closeButtons.forEach((button) => {
        button.addEventListener("click", (event) => {
          event.preventDefault();
          event.stopPropagation();
          closeModal();
        });
      });

      modal.addEventListener("click", (event) => {
        if (event.target === modal) {
          closeModal();
        }
      });

      dialog.addEventListener("keydown", (event) => {
        if (event.key === "Escape") {
          event.preventDefault();
          closeModal();
        }
      });

      refreshBtn?.addEventListener("click", () => {
        void refreshLogs();
      });
    });
  };

  const initEndpointUI = async () => {
    initScreenLogPanel();
    initSalesTabs();
    initSalesReportLoading();
    initSlackReportLoading();
    initAllowedLinesLoading();
    initJourneyUpdateTableFilters();
    initJourneyUpdateEditorForm();
    initJourneyUpdateFloatingScrollbar();
    initJourneySearchForm();
    initObusRuleDefineWorkbench();
    initObusUserCreateWorkbench();
    initStationPassengerInfoPage();
    initAllCompaniesLoading();
    initAllCompaniesObusJobMonitor();
    initMentiHelper();
    initPermissionsBulkForm();

    const modal = document.querySelector("#endpoint-modal");
    if (!modal) return;
    const openBtn = document.querySelector("#open-endpoint-modal");
    const addTargetBtn = document.querySelector("#add-target-url-button");
    const closeBtn = document.querySelector("#close-endpoint-modal");
    const modalTitle = document.querySelector("#endpoint-modal-title");
    const form = document.querySelector("#endpoint-form");
    const submitBtn = document.querySelector("#endpoint-submit-button");
    const modalFormStatus = document.querySelector("#endpoint-form-status");
    const sendBtn = document.querySelector("#send-request");
    const statusText = document.querySelector("#request-status");
    const responseStatus = document.querySelector("#response-status");
    const responseBody = document.querySelector("#response-body");
    const responseUrl = document.querySelector("#response-url");
    const responseTime = document.querySelector("#response-time");
    const targetInput = document.querySelector("#target-url-input");
    const loginProfileSelect = document.querySelector("#login-profile-select");
    const loginPartnerCodeInput = document.querySelector("#login-partner-code");
    const loginBranchIdInput = document.querySelector("#login-branch-id");
    const loginProfileNameInput = document.querySelector("#login-profile-name");
    const saveLoginProfileBtn = document.querySelector("#save-login-profile");
    const deleteLoginProfileBtn = document.querySelector("#delete-login-profile");
    const loginProfileStatus = document.querySelector("#login-profile-status");
    const historyList = document.querySelector("#request-history");
    const clearHistoryBtn = document.querySelector("#clear-history");
    const headersRows = document.querySelector("#headers-rows");
    const paramsRows = document.querySelector("#params-rows");
    const addHeaderRow = document.querySelector("#add-header-row");
    const addParamRow = document.querySelector("#add-param-row");
    const headersTextarea = document.querySelector("#endpoint-headers");
    const paramsTextarea = document.querySelector("#endpoint-params");
    const bodyTextarea = document.querySelector("#endpoint-body");
    const copyHeadersJson = document.querySelector("#copy-headers-json");
    const copyParamsJson = document.querySelector("#copy-params-json");
    const copyBodyBtn = document.querySelector("#copy-body");
    const endpointTables = [
      document.querySelector("#endpoint-table"),
      document.querySelector("#endpoint-table-inline")
    ].filter(Boolean);
    const hasHistoryPanel = Boolean(historyList);

    let endpoints = await seedIfEmpty();
    let targetUrls = await loadTargetUrls();
    let selectedTargetUrl = loadSelectedTargetUrlFromStorage();
    if (!selectedTargetUrl) {
      const legacyTargetUrl = endpoints
        .map((item) => String(item.targetUrl || "").trim())
        .find((value) => value);
      if (legacyTargetUrl) {
        selectedTargetUrl = legacyTargetUrl;
      }
    }
    let selected = Number.isInteger(Number(endpoints[0]?.id)) ? Number(endpoints[0].id) : null;
    let editingEndpointId = null;
    let draggingEndpointId = null;
    let suppressEndpointClickUntil = 0;
    let loginProfiles = loadLoginProfilesFromStorage();
    let activeLoginProfileId = "";
    let endpointLastResponses = loadEndpointLastResponsesFromStorage();

    const headerEditor = initRowEditor({
      rowsContainer: headersRows,
      addButton: addHeaderRow,
      textarea: headersTextarea,
      copyJsonButton: copyHeadersJson
    });
    const paramEditor = initRowEditor({
      rowsContainer: paramsRows,
      addButton: addParamRow,
      textarea: paramsTextarea,
      copyJsonButton: copyParamsJson
    });

    const getEndpointById = (id) => endpoints.find((item) => Number(item.id) === Number(id));

    const clearDragState = () => {
      endpointTables.forEach((table) => {
        table
          .querySelectorAll(".endpoint-row.dragging, .endpoint-row.drag-over")
          .forEach((row) => row.classList.remove("dragging", "drag-over"));
      });
    };

    const moveEndpoint = (fromId, toId) => {
      if (!Number.isInteger(Number(fromId)) || !Number.isInteger(Number(toId))) return false;
      if (Number(fromId) === Number(toId)) return false;
      const fromIndex = endpoints.findIndex((item) => Number(item.id) === Number(fromId));
      const toIndex = endpoints.findIndex((item) => Number(item.id) === Number(toId));
      if (fromIndex < 0 || toIndex < 0) return false;
      const [moved] = endpoints.splice(fromIndex, 1);
      endpoints.splice(toIndex, 0, moved);
      return true;
    };

    const setLoginProfileStatus = (message = "", kind = "") => {
      if (!loginProfileStatus) return;
      loginProfileStatus.textContent = message;
      loginProfileStatus.className = `login-profile-status${kind ? ` ${kind}` : ""}`;
    };

    const getActiveLoginProfile = () =>
      loginProfiles.find((item) => String(item.id) === String(activeLoginProfileId));

    const fillLoginProfileInputs = (profile) => {
      if (loginPartnerCodeInput) {
        loginPartnerCodeInput.value = profile?.partnerCode || "";
      }
      if (loginBranchIdInput) {
        loginBranchIdInput.value = profile?.branchId || "";
      }
      if (loginProfileNameInput) {
        loginProfileNameInput.value = profile?.name || "";
      }
    };

    const getLoginProfileLabel = (profile) =>
      profile.name || `${profile.partnerCode || "-"} / ${profile.branchId || "-"}`;

    const renderLoginProfileOptions = () => {
      if (!loginProfileSelect) return;
      const normalizedActiveId = String(activeLoginProfileId || "");
      loginProfileSelect.innerHTML = "";

      const manualOption = document.createElement("option");
      manualOption.value = "";
      manualOption.textContent = "Manuel seçim";
      loginProfileSelect.appendChild(manualOption);

      loginProfiles.forEach((profile) => {
        const option = document.createElement("option");
        option.value = profile.id;
        option.textContent = getLoginProfileLabel(profile);
        loginProfileSelect.appendChild(option);
      });

      if (normalizedActiveId && loginProfiles.some((item) => item.id === normalizedActiveId)) {
        loginProfileSelect.value = normalizedActiveId;
      } else {
        activeLoginProfileId = "";
        loginProfileSelect.value = "";
      }
    };

    const selectLoginProfile = (profileId, options = {}) => {
      const { keepManualInputs = false } = options;
      const normalizedId = String(profileId || "");
      if (normalizedId && loginProfiles.some((item) => String(item.id) === normalizedId)) {
        activeLoginProfileId = normalizedId;
      } else {
        activeLoginProfileId = "";
      }

      const profile = getActiveLoginProfile();
      if (profile) {
        fillLoginProfileInputs(profile);
      } else if (!keepManualInputs) {
        fillLoginProfileInputs(null);
      }

      renderLoginProfileOptions();
    };

    const getSelectedUserLoginVariables = () => ({
      partnerCode: String(loginPartnerCodeInput?.value || "").trim(),
      branchId: String(loginBranchIdInput?.value || "").trim()
    });

    const saveLoginProfiles = () => {
      const saved = saveLoginProfilesToStorage(loginProfiles);
      if (!saved) {
        setLoginProfileStatus("Profil kaydedilemedi.", "error");
      }
      return saved;
    };

    const getActiveTargetUrl = () => String(targetInput?.value || "").trim();

    const persistSelectedTargetUrl = (value) => {
      selectedTargetUrl = String(value || "").trim();
      saveSelectedTargetUrlToStorage(selectedTargetUrl);
      return selectedTargetUrl;
    };

    const refreshTargetOptions = (preferred = "") => {
      renderTargets(targetUrls, preferred || selectedTargetUrl || "");
      const current = getActiveTargetUrl();
      if (current) {
        persistSelectedTargetUrl(current);
      }
    };

    const setModalFormStatus = (message = "", kind = "") => {
      if (!modalFormStatus) return;
      modalFormStatus.textContent = message;
      modalFormStatus.className = `form-feedback${kind ? ` ${kind}` : ""}`;
      modalFormStatus.hidden = !message;
    };

    const setModalMode = (mode, item = null) => {
      const isEdit = mode === "edit" && item;
      editingEndpointId = isEdit ? Number(item.id) : null;
      setModalFormStatus("", "");

      if (modalTitle) {
        modalTitle.textContent = isEdit ? "Endpoint Düzenle" : "Endpoint Ekle";
      }
      if (submitBtn) {
        submitBtn.textContent = isEdit ? "Güncelle" : "Kaydet";
      }

      if (!form) return;
      const titleField = form.elements.namedItem("title");
      const methodField = form.elements.namedItem("method");
      const pathField = form.elements.namedItem("path");
      const descriptionField = form.elements.namedItem("description");

      if (!isEdit) {
        form.reset();
        return;
      }

      if (titleField) titleField.value = item.title || "";
      if (methodField) methodField.value = (item.method || "GET").toUpperCase();
      if (pathField) pathField.value = item.path || "/";
      if (descriptionField) descriptionField.value = item.description || "";
    };

    const renderSelected = async (endpointId) => {
      if (!Number.isInteger(Number(endpointId))) return;
      selected = Number(endpointId);
      renderTable(endpoints, selected);
      refreshTargetOptions();
      const current = endpoints.find((e) => Number(e.id) === selected);
      if (!current) return;
      renderDetails(current, { headers: headerEditor, params: paramEditor });
      showStoredResponseForEndpoint(selected);
      if (hasHistoryPanel) {
        renderHistory(await loadRequests(selected));
        renderHistoryDetail(null);
      }
    };

    renderTable(endpoints, selected);
    refreshTargetOptions(selectedTargetUrl || (targetUrls[0]?.url || ""));
    if (selected !== null) {
      const current = endpoints.find((e) => Number(e.id) === selected);
      if (current) {
        renderDetails(current, { headers: headerEditor, params: paramEditor });
      }
    }
    if (hasHistoryPanel) {
      renderHistory(await loadRequests(selected));
      renderHistoryDetail(null);
    }

    if (loginProfiles.length) {
      selectLoginProfile(loginProfiles[0].id);
    } else {
      selectLoginProfile("", { keepManualInputs: true });
    }
    setLoginProfileStatus("");

    const openModal = () => {
      modal.classList.add("active");
      modal.setAttribute("aria-hidden", "false");
    };

    const closeModal = () => {
      modal.classList.remove("active");
      modal.setAttribute("aria-hidden", "true");
      setModalMode("create");
    };

    openBtn?.addEventListener("click", (event) => {
      event.preventDefault();
      event.stopPropagation();
      setModalMode("create");
      openModal();
    });

    addTargetBtn?.addEventListener("click", async (event) => {
      event.preventDefault();
      event.stopPropagation();
      const initialValue = getActiveTargetUrl() || selectedTargetUrl || "https://";
      const draft = window.prompt("Kaydedilecek Hedef URL", initialValue);
      if (draft === null) return;
      let nextUrl = String(draft || "").trim();
      if (!nextUrl) {
        if (statusText) statusText.textContent = "Hedef URL boş olamaz.";
        return;
      }
      if (!/^[a-z][a-z0-9+.-]*:\/\//i.test(nextUrl)) {
        nextUrl = `https://${nextUrl}`;
      }
      if (statusText) statusText.textContent = "Hedef URL kaydediliyor...";
      const { item, error } = await saveTargetUrlRecord(nextUrl);
      if (!item) {
        if (statusText) statusText.textContent = error || "Hedef URL kaydedilemedi.";
        return;
      }
      targetUrls = await loadTargetUrls();
      const savedUrl = String(item.url || "").trim();
      persistSelectedTargetUrl(savedUrl);
      refreshTargetOptions(savedUrl);
      if (statusText) statusText.textContent = "Hedef URL kaydedildi.";
    });
    closeBtn?.addEventListener("click", (event) => {
      event.preventDefault();
      closeModal();
    });
    modal.addEventListener("click", (event) => {
      if (event.target === modal) {
        closeModal();
      }
    });
    if (window.location.hash === "#endpoint-modal") {
      setModalMode("create");
      openModal();
    }

    const onEndpointClick = (event) => {
      if (Date.now() < suppressEndpointClickUntil) {
        return;
      }
      const table = event.currentTarget;
      const editBtn = event.target.closest(".endpoint-edit[data-endpoint-id]");
      if (editBtn) {
        const current = getEndpointById(editBtn.dataset.endpointId);
        if (!current) return;
        setModalMode("edit", current);
        openModal();
        return;
      }
      if (table?.id === "endpoint-table") {
        return;
      }
      const row = event.target.closest(".endpoint-row[data-endpoint-id]");
      if (!row) return;
      renderSelected(row.dataset.endpointId);
    };
    endpointTables.forEach((table) => table.addEventListener("click", onEndpointClick));

    const onEndpointDragStart = (event) => {
      const row = event.target.closest(".endpoint-row.selectable[data-endpoint-id]");
      if (!row || event.target.closest(".endpoint-edit")) return;
      const endpointId = Number(row.dataset.endpointId);
      if (!Number.isInteger(endpointId)) return;
      draggingEndpointId = endpointId;
      clearDragState();
      row.classList.add("dragging");
      if (event.dataTransfer) {
        event.dataTransfer.effectAllowed = "move";
        event.dataTransfer.setData("text/plain", String(endpointId));
      }
    };

    const onEndpointDragOver = (event) => {
      if (!Number.isInteger(draggingEndpointId)) return;
      const row = event.target.closest(".endpoint-row.selectable[data-endpoint-id]");
      if (!row) return;
      const overId = Number(row.dataset.endpointId);
      if (!Number.isInteger(overId) || overId === draggingEndpointId) return;
      event.preventDefault();
      if (event.dataTransfer) {
        event.dataTransfer.dropEffect = "move";
      }
      endpointTables.forEach((table) => {
        table.querySelectorAll(".endpoint-row.drag-over").forEach((item) => {
          item.classList.remove("drag-over");
        });
      });
      row.classList.add("drag-over");
    };

    const onEndpointDrop = async (event) => {
      if (!Number.isInteger(draggingEndpointId)) return;
      const row = event.target.closest(".endpoint-row.selectable[data-endpoint-id]");
      if (!row) return;
      event.preventDefault();
      const dropId = Number(row.dataset.endpointId);
      const moved = moveEndpoint(draggingEndpointId, dropId);
      clearDragState();
      draggingEndpointId = null;
      if (!moved) return;

      suppressEndpointClickUntil = Date.now() + 250;
      renderTable(endpoints, selected);
      refreshTargetOptions();

      const orderedIds = endpoints
        .map((item) => Number(item.id))
        .filter((id) => Number.isInteger(id));
      const { ok, items, error } = await reorderEndpointList(orderedIds);
      if (!ok) {
        if (statusText) statusText.textContent = error || "Endpoint sıralaması kaydedilemedi.";
        const refreshed = normalizeEndpoints(await loadEndpoints());
        if (refreshed.length) {
          endpoints = refreshed;
        }
        renderTable(endpoints, selected);
        refreshTargetOptions();
        return;
      }

      if (Array.isArray(items) && items.length) {
        endpoints = normalizeEndpoints(items);
      }
      renderTable(endpoints, selected);
      refreshTargetOptions();
      if (statusText) statusText.textContent = "Endpoint sırası güncellendi.";
    };

    const onEndpointDragEnd = () => {
      clearDragState();
      draggingEndpointId = null;
    };

    endpointTables.forEach((table) => {
      table.addEventListener("dragstart", onEndpointDragStart);
      table.addEventListener("dragover", onEndpointDragOver);
      table.addEventListener("drop", onEndpointDrop);
      table.addEventListener("dragend", onEndpointDragEnd);
    });

    loginProfileSelect?.addEventListener("change", () => {
      selectLoginProfile(loginProfileSelect.value, { keepManualInputs: true });
      setLoginProfileStatus("");
    });

    saveLoginProfileBtn?.addEventListener("click", () => {
      const partnerCode = String(loginPartnerCodeInput?.value || "").trim();
      const branchId = String(loginBranchIdInput?.value || "").trim();
      const profileName = String(loginProfileNameInput?.value || "").trim();

      if (!partnerCode || !branchId) {
        setLoginProfileStatus("partner-code ve branch-id zorunlu.", "error");
        return;
      }

      const normalizedPartnerCode = partnerCode.toLowerCase();
      const normalizedBranchId = branchId.toLowerCase();
      const existingProfile = loginProfiles.find(
        (item) =>
          String(item.partnerCode || "").trim().toLowerCase() === normalizedPartnerCode &&
          String(item.branchId || "").trim().toLowerCase() === normalizedBranchId
      );
      if (existingProfile) {
        selectLoginProfile(existingProfile.id, { keepManualInputs: true });
        setLoginProfileStatus("Bu partner-code ve branch-id zaten kayıtlı.", "error");
        return;
      }

      activeLoginProfileId = `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
      loginProfiles.push({
        id: activeLoginProfileId,
        name: profileName || `${partnerCode} / ${branchId}`,
        partnerCode,
        branchId
      });

      if (!saveLoginProfiles()) return;
      selectLoginProfile(activeLoginProfileId, { keepManualInputs: true });
      setLoginProfileStatus("Profil kaydedildi.", "success");
    });

    deleteLoginProfileBtn?.addEventListener("click", () => {
      if (!activeLoginProfileId) {
        setLoginProfileStatus("Silinecek kayıt seçilmedi.", "error");
        return;
      }
      loginProfiles = loginProfiles.filter((item) => item.id !== activeLoginProfileId);
      activeLoginProfileId = "";
      if (!saveLoginProfiles()) return;
      if (loginProfiles.length) {
        selectLoginProfile(loginProfiles[0].id);
      } else {
        selectLoginProfile("", { keepManualInputs: false });
      }
      setLoginProfileStatus("Profil silindi.", "success");
    });

    [loginPartnerCodeInput, loginBranchIdInput, loginProfileNameInput]
      .filter(Boolean)
      .forEach((input) => {
        input.addEventListener("input", () => setLoginProfileStatus(""));
      });

    form?.addEventListener("submit", async (event) => {
      event.preventDefault();
      setModalFormStatus("", "");
      if (submitBtn) submitBtn.disabled = true;
      try {
        const data = new FormData(form);
        const rawPath = data.get("path")?.toString().trim() || "";
        if (!rawPath) {
          const msg = "Endpoint URL zorunlu.";
          if (statusText) statusText.textContent = msg;
          setModalFormStatus(msg, "error");
          return;
        }
        const normalized = normalizeEndpointAddress({
          targetUrl: "",
          path: rawPath
        });

        const item = {
          title: data.get("title")?.toString().trim() || "Endpoint",
          method: data.get("method")?.toString().toUpperCase() || "GET",
          path: normalized.path,
          description: data.get("description")?.toString().trim() || "",
          targetUrl: null,
          body: defaultBody,
          headers: defaultHeaders,
          params: defaultParams
        };

        const isEdit = Number.isInteger(editingEndpointId);
        if (isEdit) {
          const current = getEndpointById(editingEndpointId);
          if (!current) {
            const msg = "Düzenlenecek endpoint bulunamadı.";
            if (statusText) statusText.textContent = msg;
            setModalFormStatus(msg, "error");
            return;
          }
          const updated = await updateEndpoint(Number(editingEndpointId), {
            ...item,
            body: current.body || defaultBody,
            headers: current.headers || defaultHeaders,
            params: current.params || defaultParams
          });
          if (!updated) {
            const msg = "Endpoint güncellenemedi.";
            if (statusText) statusText.textContent = msg;
            setModalFormStatus(msg, "error");
            return;
          }
          const refreshed = normalizeEndpoints(await loadEndpoints());
          if (refreshed.length) {
            endpoints = refreshed;
          }
          selected = Number(editingEndpointId);
          renderTable(endpoints, selected);
          refreshTargetOptions();
          await renderSelected(selected);
          if (statusText) statusText.textContent = "Endpoint güncellendi.";
          setModalFormStatus("Kayıt başarılı.", "success");
          setTimeout(() => {
            closeModal();
            setModalFormStatus("", "");
          }, 500);
          return;
        }

        const { item: created, error: saveError } = await saveEndpoint(item);
        if (!created) {
          const msg = saveError || "Endpoint kaydedilemedi.";
          if (statusText) statusText.textContent = msg;
          setModalFormStatus(msg, "error");
          return;
        }
        const refreshed = normalizeEndpoints(await loadEndpoints());
        if (refreshed.length) {
          endpoints = refreshed;
        } else {
          const deduped = endpoints.filter((entry) => Number(entry.id) !== Number(created.id));
          endpoints = normalizeEndpoints([created, ...deduped]);
        }

        selected = Number.isInteger(Number(created.id)) ? Number(created.id) : null;
        if (selected === null) {
          selected = Number.isInteger(Number(endpoints[0]?.id)) ? Number(endpoints[0].id) : null;
        }
        renderTable(endpoints, selected);
        refreshTargetOptions();
        const current = endpoints.find((e) => Number(e.id) === Number(selected));
        if (current) {
          renderDetails(current, { headers: headerEditor, params: paramEditor });
          showStoredResponseForEndpoint(selected);
        }
        if (hasHistoryPanel) {
          renderHistory(await loadRequests(selected));
          renderHistoryDetail(null);
        }
        if (statusText) statusText.textContent = "Endpoint kaydedildi.";
        setModalFormStatus("Kayıt başarılı.", "success");
        form.reset();
        setTimeout(() => {
          closeModal();
          setModalFormStatus("", "");
        }, 500);
      } catch (err) {
        const msg = err?.message || "Endpoint kaydedilemedi.";
        if (statusText) statusText.textContent = msg;
        setModalFormStatus(msg, "error");
      } finally {
        if (submitBtn) submitBtn.disabled = false;
      }
    });

    const bindSave = (field, key) => {
      let timer;
      field?.addEventListener("input", () => {
        const current = endpoints.find((e) => Number(e.id) === Number(selected));
        if (!current || !Number.isInteger(Number(current.id))) return;
        current[key] = field.value;
        clearTimeout(timer);
        timer = setTimeout(() => {
          updateEndpoint(current.id, {
            body: current.body,
            headers: current.headers,
            params: current.params
          });
        }, 500);
      });
    };

    bindSave(document.querySelector("#endpoint-headers"), "headers");
    bindSave(document.querySelector("#endpoint-params"), "params");
    bindSave(bodyTextarea, "body");

    if (bodyTextarea && !bodyTextarea.value.trim()) {
      bodyTextarea.value = defaultBody;
    }
    copyBodyBtn?.addEventListener("click", async () => {
      const text = bodyTextarea?.value || defaultBody;
      if (navigator.clipboard?.writeText) {
        await navigator.clipboard.writeText(text);
      } else {
        window.prompt("Kopyala:", text);
      }
    });

    targetInput?.addEventListener("change", () => {
      persistSelectedTargetUrl(getActiveTargetUrl());
    });
    targetInput?.addEventListener("focus", async () => {
      targetUrls = await loadTargetUrls();
      refreshTargetOptions(selectedTargetUrl || getActiveTargetUrl());
    });

    const getDefaultResponseState = () => ({
      statusText: "",
      badgeText: "Bekleniyor",
      badgeClass: "muted",
      body: "{}",
      url: "",
      time: ""
    });

    const normalizeResponseState = (state = {}) => {
      const defaults = getDefaultResponseState();
      return {
        statusText: String(state?.statusText ?? defaults.statusText),
        badgeText: String(state?.badgeText ?? defaults.badgeText),
        badgeClass: String(state?.badgeClass ?? defaults.badgeClass),
        body: String(state?.body ?? defaults.body),
        url: String(state?.url ?? defaults.url),
        time: String(state?.time ?? defaults.time)
      };
    };

    const setResponseState = (state = {}) => {
      const nextState = normalizeResponseState(state);
      if (statusText) statusText.textContent = nextState.statusText;
      if (responseStatus) {
        responseStatus.textContent = nextState.badgeText;
        responseStatus.className = `pill ${nextState.badgeClass || "muted"}`.trim();
      }
      if (responseBody) responseBody.textContent = nextState.body;
      if (responseUrl) responseUrl.textContent = nextState.url;
      if (responseTime) responseTime.textContent = nextState.time;
    };

    const saveLastResponseForEndpoint = (endpointId, state) => {
      if (!Number.isInteger(Number(endpointId))) return;
      endpointLastResponses[String(Number(endpointId))] = normalizeResponseState(state);
      saveEndpointLastResponsesToStorage(endpointLastResponses);
    };

    const showStoredResponseForEndpoint = (endpointId) => {
      if (!Number.isInteger(Number(endpointId))) {
        setResponseState(getDefaultResponseState());
        return;
      }
      const saved = endpointLastResponses[String(Number(endpointId))];
      if (!saved) {
        setResponseState(getDefaultResponseState());
        return;
      }
      setResponseState(saved);
    };

    showStoredResponseForEndpoint(selected);

    const parseJsonField = (value, label) => {
      if (!value || !value.trim()) return {};
      try {
        return JSON.parse(value);
      } catch (err) {
        throw new Error(`${label} JSON hatalı.`);
      }
    };

    const normalizeTokenName = (value) =>
      String(value || "")
        .toLowerCase()
        .replace(/[^a-z0-9]/g, "");

    const applyTemplateValue = (input, variables = {}) => {
      if (typeof input !== "string") return input;
      return input.replace(/\{\{\s*([^}]+?)\s*\}\}/g, (match, token) => {
        const normalizedToken = normalizeTokenName(token);
        if (normalizedToken === "sessionid" && variables.sessionId !== undefined) {
          return String(variables.sessionId);
        }
        if (normalizedToken === "deviceid" && variables.deviceId !== undefined) {
          return String(variables.deviceId);
        }
        if (normalizedToken === "partnercode" && variables.partnerCode !== undefined) {
          return String(variables.partnerCode);
        }
        if (normalizedToken === "branchid" && variables.branchId !== undefined) {
          return String(variables.branchId);
        }
        return match;
      });
    };

    const applyTemplateObject = (value, variables = {}) => {
      if (typeof value === "string") {
        return applyTemplateValue(value, variables);
      }
      if (Array.isArray(value)) {
        return value.map((item) => applyTemplateObject(item, variables));
      }
      if (value && typeof value === "object") {
        return Object.entries(value).reduce((acc, [key, item]) => {
          acc[key] = applyTemplateObject(item, variables);
          return acc;
        }, {});
      }
      return value;
    };

    const getCurrentEndpoint = () =>
      endpoints.find((item) => Number(item.id) === Number(selected)) ||
      endpoints.find((item) => Number.isInteger(Number(item.id)));

    const getEndpointSearchText = (item) =>
      `${item?.title || ""} ${item?.path || ""} ${item?.description || ""}`.toLowerCase();

    const findEndpointByToken = (token) =>
      endpoints.find((item) => getEndpointSearchText(item).includes(String(token || "").toLowerCase()));

    const buildPayloadForEndpoint = (endpoint, options = {}) => {
      const endpointId = Number(endpoint?.id);
      if (!Number.isInteger(endpointId)) {
        throw new Error("Endpoint bulunamadı.");
      }
      const {
        templateVariables = {},
        targetUrlOverride = "",
        preferEditorValues = false
      } = options;
      const endpointName = endpoint.title || endpoint.path || "Endpoint";
      const method = (endpoint.method || "GET").toUpperCase();
      const paramsField = document.querySelector("#endpoint-params");

      const headersSource = preferEditorValues
        ? document.querySelector("#endpoint-headers")?.value || endpoint.headers || ""
        : endpoint.headers || "";
      const paramsSource = paramsField
        ? preferEditorValues
          ? paramsField.value || endpoint.params || ""
          : endpoint.params || ""
        : defaultParams;
      const bodySource = preferEditorValues ? bodyTextarea?.value || endpoint.body || "" : endpoint.body || "";

      const normalized = normalizeEndpointAddress({
        targetUrl: targetUrlOverride || "",
        path: endpoint.path || "/"
      });
      if (!normalized.targetUrl) {
        throw new Error("Hedef URL seçilmeli.");
      }

      const headers = applyTemplateObject(
        parseJsonField(headersSource, `${endpointName} Headers`),
        templateVariables
      );
      const params = applyTemplateObject(
        parseJsonField(paramsSource, `${endpointName} Params`),
        templateVariables
      );
      const body = applyTemplateValue(bodySource.trim() ? bodySource : "", templateVariables);

      return {
        endpointId,
        method,
        path: normalized.path,
        targetUrl: normalized.targetUrl,
        headers,
        params,
        body
      };
    };

    const executePayload = async (payload) => {
      try {
        const response = await fetch("/api/execute", {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            Accept: "application/json"
          },
          body: JSON.stringify(payload)
        });

        let data = null;
        const contentType = response.headers.get("content-type") || "";
        if (contentType.includes("application/json")) {
          try {
            data = await response.json();
          } catch (err) {
            data = null;
          }
        } else {
          const text = await response.text();
          data = {
            ok: false,
            error: `Sunucu yanıtı JSON değil (${response.status})`,
            details: text || ""
          };
        }

        if (!response.ok || !data || data.error) {
          return {
            ok: false,
            error: data?.error || `İstek başarısız (${response.status}).`,
            data: data || null,
            details: data?.details || data?.body || "{}"
          };
        }
        return {
          ok: true,
          data
        };
      } catch (err) {
        return {
          ok: false,
          error: "İstek hatası.",
          details: err.message || "{}",
          data: null
        };
      }
    };

    const ensureSuccessfulStep = (stepName, result) => {
      if (!result.ok) {
        throw new Error(`${stepName} başarısız: ${result.error || "İstek tamamlanamadı."}`);
      }
      if (!result.data?.ok) {
        const statusCode = Number(result.data?.status) || 0;
        const statusTextLabel = result.data?.statusText || "Yanıt hatalı";
        throw new Error(`${stepName} başarısız: ${statusCode} ${statusTextLabel}`.trim());
      }
      return result.data;
    };

    const findNestedValue = (node, keySet) => {
      if (!node) return undefined;
      if (Array.isArray(node)) {
        for (const item of node) {
          const found = findNestedValue(item, keySet);
          if (found !== undefined) return found;
        }
        return undefined;
      }
      if (typeof node !== "object") return undefined;

      for (const [key, value] of Object.entries(node)) {
        if (keySet.has(normalizeTokenName(key)) && value !== undefined && value !== null && `${value}`.trim()) {
          return String(value);
        }
      }
      for (const value of Object.values(node)) {
        const found = findNestedValue(value, keySet);
        if (found !== undefined) return found;
      }
      return undefined;
    };

    const extractSessionVariables = (responseBody) => {
      if (!responseBody || typeof responseBody !== "string") {
        return { sessionId: "", deviceId: "" };
      }
      let parsed = null;
      try {
        parsed = JSON.parse(responseBody);
      } catch (err) {
        parsed = null;
      }
      if (!parsed || typeof parsed !== "object") {
        return { sessionId: "", deviceId: "" };
      }
      const sessionId = findNestedValue(parsed, new Set(["sessionid"])) || "";
      const deviceId = findNestedValue(parsed, new Set(["deviceid"])) || "";
      return { sessionId, deviceId };
    };

    const prettifyResponse = (payload) => {
      try {
        return JSON.stringify(JSON.parse(payload), null, 2);
      } catch (err) {
        return payload || "";
      }
    };

    sendBtn?.addEventListener("click", async () => {
      let currentEndpointId = Number(selected);
      setResponseState({
        statusText: "İstek gönderiliyor...",
        badgeText: "İşleniyor",
        badgeClass: "muted",
        body: "..."
      });

      try {
        const current = getCurrentEndpoint();
        if (!current || !Number.isInteger(Number(current.id))) {
          throw new Error("Endpoint bulunamadı.");
        }
        currentEndpointId = Number(current.id);
        const activeTargetUrl = getActiveTargetUrl();
        if (!activeTargetUrl) {
          throw new Error("Hedef URL seçilmeli.");
        }

        let templateVariables = {};
        const currentSearchText = getEndpointSearchText(current);
        const isUserLogin = currentSearchText.includes("userlogin");
        const isGetStations = currentSearchText.includes("getstation");
        const requiresSessionSetup = isUserLogin || isGetStations;

        if (isUserLogin) {
          const selectedUserLoginVariables = getSelectedUserLoginVariables();
          if (!selectedUserLoginVariables.partnerCode || !selectedUserLoginVariables.branchId) {
            throw new Error("UserLogin için partner-code ve branch-id seçilmeli.");
          }
          templateVariables = {
            ...templateVariables,
            ...selectedUserLoginVariables
          };
        }

        if (requiresSessionSetup) {
          setResponseState({
            statusText: "GetSession çalıştırılıyor...",
            badgeText: "Ön Hazırlık",
            badgeClass: "muted",
            body: "..."
          });

          const getSessionEndpoint = findEndpointByToken("getsession");
          if (!getSessionEndpoint) {
            throw new Error("GetSession endpoint'i bulunamadı.");
          }
          const getSessionPayload = buildPayloadForEndpoint(getSessionEndpoint, {
            preferEditorValues: Number(getSessionEndpoint.id) === Number(selected),
            targetUrlOverride: activeTargetUrl
          });
          const getSessionResult = await executePayload(getSessionPayload);
          const getSessionData = ensureSuccessfulStep("GetSession", getSessionResult);
          const extracted = extractSessionVariables(getSessionData.body || "");
          if (!extracted.sessionId || !extracted.deviceId) {
            throw new Error("GetSession yanıtında session-id ve device-id bulunamadı.");
          }
          templateVariables = {
            ...templateVariables,
            ...extracted
          };
        }

        if (isUserLogin) {
          setResponseState({
            statusText: "GetParameter çalıştırılıyor...",
            badgeText: "Ön Hazırlık",
            badgeClass: "muted",
            body: "..."
          });

          const getParameterEndpoint = findEndpointByToken("getparameter");
          if (!getParameterEndpoint) {
            throw new Error("GetParameter endpoint'i bulunamadı.");
          }
          const getParameterPayload = buildPayloadForEndpoint(getParameterEndpoint, {
            templateVariables,
            preferEditorValues: Number(getParameterEndpoint.id) === Number(selected),
            targetUrlOverride: activeTargetUrl
          });
          const getParameterResult = await executePayload(getParameterPayload);
          ensureSuccessfulStep("GetParameter", getParameterResult);

          setResponseState({
            statusText: "UserLogin çalıştırılıyor...",
            badgeText: "İşleniyor",
            badgeClass: "muted",
            body: "..."
          });
        } else if (isGetStations) {
          setResponseState({
            statusText: "GetStations çalıştırılıyor...",
            badgeText: "İşleniyor",
            badgeClass: "muted",
            body: "..."
          });
        }

        const payload = buildPayloadForEndpoint(current, {
          templateVariables,
          preferEditorValues: true,
          targetUrlOverride: activeTargetUrl
        });
        const execution = await executePayload(payload);
        if (!execution.ok || !execution.data) {
          const errorState = {
            statusText: execution.error || "İstek başarısız.",
            badgeText: "Hata",
            badgeClass: "muted",
            body: execution.details || "{}"
          };
          setResponseState(errorState);
          saveLastResponseForEndpoint(currentEndpointId, errorState);
          return;
        }

        const data = execution.data;
        const formatted = prettifyResponse(data.body || "");
        const badgeClass = data.ok ? "success" : "muted";
        const allowHeader = data?.headers?.allow || data?.headers?.Allow || "";
        let statusLine = data.ok ? "Tamamlandı" : "Yanıt hata döndü";
        if (!data.ok && Number(data.status) === 405) {
          statusLine = allowHeader
            ? `405 Method Not Allowed (Allow: ${allowHeader})`
            : "405 Method Not Allowed (method/path kontrol et)";
        }
        const successState = {
          statusText: statusLine,
          badgeText: `${data.status} ${data.statusText}`,
          badgeClass,
          body: formatted || "{}",
          url: data.url ? `URL: ${data.url}` : "",
          time: data.durationMs ? `Süre: ${data.durationMs} ms` : ""
        };
        setResponseState(successState);
        saveLastResponseForEndpoint(currentEndpointId, successState);
        if (hasHistoryPanel) {
          loadRequests(Number(current.id)).then(renderHistory);
        }
      } catch (err) {
        const errorState = {
          statusText: err.message || "İstek hatası.",
          badgeText: "Hata",
          badgeClass: "muted",
          body: "{}"
        };
        setResponseState(errorState);
        saveLastResponseForEndpoint(currentEndpointId, errorState);
      }
    });

    historyList?.addEventListener("click", async (event) => {
      const item = event.target.closest(".history-item");
      if (!item) return;
      const id = Number(item.dataset.requestId);
      if (!Number.isInteger(id)) return;
      const detail = await loadRequestDetail(id);
      renderHistoryDetail(detail);
    });

    clearHistoryBtn?.addEventListener("click", async () => {
      if (!selected) return;
      const ok = window.confirm("Seçili endpoint geçmişi silinsin mi?");
      if (!ok) return;
      const cleared = await clearRequests(selected);
      if (cleared) {
        renderHistory([]);
        renderHistoryDetail(null);
      }
    });

    initTabs();

  };

  initEndpointUI();
})();
