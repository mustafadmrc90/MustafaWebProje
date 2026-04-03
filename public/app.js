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
  const legacyObusBulkUserTemplatesStorageKey = "obus_bulk_user_templates_v1";
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

  const normalizeObusBulkUserTemplateItems = (items) =>
    (Array.isArray(items) ? items : [])
      .map((item) => {
        const entries = Array.isArray(item?.entries)
          ? item.entries
              .map((entry) => ({
                fullName: String(entry?.fullName || "").trim(),
                username: String(entry?.username || "").trim(),
                password: String(entry?.password || "")
              }))
              .filter((entry) => entry.fullName || entry.username || entry.password)
          : [];
        return {
          id: String(item?.id || "").trim(),
          name: String(item?.name || "").trim(),
          createdAt: Number(item?.createdAt || 0),
          updatedAt: Number(item?.updatedAt || item?.createdAt || 0),
          createdByName: String(item?.createdByName || "").trim(),
          updatedByName: String(item?.updatedByName || item?.createdByName || "").trim(),
          entries
        };
      })
      .filter((item) => item.id && item.name && item.entries.length > 0)
      .sort((a, b) => Number(b.updatedAt || 0) - Number(a.updatedAt || 0));

  const loadLegacyObusBulkUserTemplatesFromStorage = () => {
    try {
      const raw = window.localStorage.getItem(legacyObusBulkUserTemplatesStorageKey);
      if (!raw) return [];
      const parsed = JSON.parse(raw);
      return normalizeObusBulkUserTemplateItems(parsed);
    } catch (err) {
      return [];
    }
  };

  const clearLegacyObusBulkUserTemplatesFromStorage = () => {
    try {
      window.localStorage.removeItem(legacyObusBulkUserTemplatesStorageKey);
      return true;
    } catch (err) {
      return false;
    }
  };

  const loadObusBulkUserTemplates = async (url) => {
    const endpointUrl = String(url || "").trim() || "/api/obus-user-create-bulk/templates";
    try {
      const response = await fetch(endpointUrl, {
        headers: { Accept: "application/json" }
      });
      const data = await parseJsonResponse(response);
      if (!response.ok || !data?.ok) {
        return {
          items: [],
          error: getApiErrorMessage(response, data, "Şablonlar alınamadı")
        };
      }
      return {
        items: normalizeObusBulkUserTemplateItems(data.items),
        error: null
      };
    } catch (err) {
      return {
        items: [],
        error: err?.message || "Şablonlar alınamadı."
      };
    }
  };

  const saveObusBulkUserTemplate = async (url, payload) => {
    const endpointUrl = String(url || "").trim() || "/api/obus-user-create-bulk/templates";
    try {
      const response = await fetch(endpointUrl, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Accept: "application/json"
        },
        body: JSON.stringify(payload || {})
      });
      const data = await parseJsonResponse(response);
      if (!response.ok || !data?.ok) {
        return {
          item: null,
          error: getApiErrorMessage(response, data, "Şablon kaydedilemedi")
        };
      }
      return {
        item: normalizeObusBulkUserTemplateItems([data.item])[0] || null,
        error: null
      };
    } catch (err) {
      return {
        item: null,
        error: err?.message || "Şablon kaydedilemedi."
      };
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
    const submitActionInput = form.querySelector("#allowed-lines-submit-action");
    const companySourceUrl = String(form.dataset.companySourceUrl || "").trim();
    if (!submitButtons.length) return;

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
      if (!selectedCluster) return;

      const baseUrl = companySourceUrl || endpointInput.value || "";
      const nextUrl = replaceClusterInUrl(baseUrl, selectedCluster);
      if (nextUrl) {
        endpointInput.value = nextUrl;
      }
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
      }
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
            setStatus("GetJourneys akışı için önce GetStation bölümünde firma seçin.", "error");
            return false;
          }
          if (!selectedJourneyOriginId || !selectedJourneyDestinationId) {
            setStatus("Kalkış ve varış istasyonlarını GetStation bölümünde seçin.", "muted");
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
        return `${id} - ${name}`;
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
          createJourneyResultCell("Ücret", getJourneyPriceValue(journey?.price?.internet, journey?.price?.currency))
        );

        journeysResultsEl.appendChild(rowEl);
      });
    };

    const buildJourneySearchErrorMessage = (response, data, rawText) => {
      const lines = [];
      const baseMessage = String(data?.error || "").trim() || `GetJourneys alınamadı (${response?.status || 0})`;
      lines.push(baseMessage);

      if (data?.step) {
        lines.push(`Adım: ${String(data.step)}`);
      }
      if (data?.requestUrl) {
        lines.push(`URL: ${String(data.requestUrl)}`);
      }
      if (data?.details) {
        lines.push(`Detay: ${String(data.details)}`);
      } else {
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
        setStatus("Kalkış ve varış istasyonlarını GetStation bölümünde seçin.", "muted");
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
        setStatus("Kalkış ve varış istasyonlarını GetStation bölümünde seçin.", "error");
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
      const baseMessage = String(data?.error || "").trim() || `İstasyonlar alınamadı (${response?.status || 0})`;
      lines.push(baseMessage);

      if (data?.step) {
        lines.push(`Adım: ${String(data.step)}`);
      }
      if (data?.requestUrl) {
        lines.push(`URL: ${String(data.requestUrl)}`);
      }
      if (data?.details) {
        lines.push(`Detay: ${String(data.details)}`);
      } else {
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
        return `${id} - ${name}`;
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

    const syncJourneysCompanySelection = () => {
      if (!journeysCompanySelect) return;
      journeysCompanySelect.value = companySelect.value || "";
    };

    const resetStationSelects = (placeholder) => {
      loadedStationItems = [];
      selectedJourneyOriginId = "";
      selectedJourneyDestinationId = "";
      clearJourneysResults();
      fillSelect(originSelect, [], placeholder || "Önce firma seçin", "");
      fillSelect(destinationSelect, [], placeholder || "Önce firma seçin", "");
      renderJourneysStationSelects();
      syncJourneysCompanySelection();
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
        syncJourneysCompanySelection();
        setStatus(`${items.length} istasyon yüklendi.`, "success");
      } catch (err) {
        if (currentSequence !== requestSequence) return;
        resetStationSelects("İstasyonlar yüklenemedi");
        setStatus(err?.message || "İstasyonlar alınamadı.", "error");
      }
    };

    companySelect.addEventListener("change", () => {
      selectedOriginValue = "";
      selectedDestinationValue = "";
      selectedJourneyOriginId = "";
      selectedJourneyDestinationId = "";
      selectedJourneysDate = String(journeysDateInput.value || "").trim();
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

    syncJourneysCompanySelection();
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

  const initObusUserCreateBuilder = () => {
    const form = document.querySelector(".obus-user-create-form");
    if (!form) return;
    if (form.dataset.builderBound === "1") return;
    form.dataset.builderBound = "1";

    const multiselect = form.querySelector(".obus-company-multiselect");
    const trigger = form.querySelector("#obus-company-trigger");
    const dropdown = form.querySelector("#obus-company-dropdown");
    const selectAllCheckbox = form.querySelector("[data-select-all='1']");
    const companyCheckboxes = Array.from(form.querySelectorAll("[data-company-checkbox='1']"));
    const selectedCompaniesInput = form.querySelector("#obus-user-create-selected-companies");
    const loadingMessage = form.querySelector(".obus-user-create-loading-message");
    const submitBtn = form.querySelector(".obus-user-create-actions button[type='submit']");
    const liveMessage = form.querySelector("[data-obus-user-create-live-message='1']");
    const liveResultsSection = document.querySelector("[data-obus-user-create-live-results='1']");
    const liveBody = liveResultsSection?.querySelector("[data-obus-user-create-live-body='1']");
    const liveProgress = liveResultsSection?.querySelector("[data-obus-create-live-progress='1']");
    const liveSuccess = liveResultsSection?.querySelector("[data-obus-create-live-success='1']");
    const liveFailure = liveResultsSection?.querySelector("[data-obus-create-live-failure='1']");
    const fullNameInput = form.querySelector("input[name='fullName']");
    const usernameInput = form.querySelector("input[name='username']");
    const passwordInput = form.querySelector("input[name='password']");
    const bulkInput = form.querySelector("input[name='bulk']");
    const isBulkMode = String(bulkInput?.value || "0").trim() === "1";
    const liveStartUrl =
      String(form.getAttribute("data-obus-live-start-url") || "").trim() || "/api/obus-user-create/live/start";
    const bulkCheckUrl =
      String(form.getAttribute("data-obus-bulk-check-url") || "").trim() || "/api/obus-user-create-bulk/check";
    const bulkTemplatesApiUrl =
      String(form.getAttribute("data-obus-bulk-templates-url") || "").trim() || "/api/obus-user-create-bulk/templates";
    const bulkUserList = form.querySelector("[data-obus-bulk-user-list='1']");
    const bulkAddRowBtn = form.querySelector("[data-obus-bulk-add-row='1']");
    const bulkCreateBtn = form.querySelector("[data-obus-bulk-create-button='1']");
    const bulkTemplateSaveOpenBtn = form.querySelector("[data-obus-bulk-template-save-open='1']");
    const bulkTemplateLoadOpenBtn = form.querySelector("[data-obus-bulk-template-load-open='1']");
    const bulkResultsSection = document.querySelector("[data-obus-bulk-check-results='1']");
    const bulkResultsBody = bulkResultsSection?.querySelector("[data-obus-bulk-check-body='1']");
    const bulkFilterSelect = bulkResultsSection?.querySelector("[data-obus-bulk-filter='1']");
    const bulkSelectAllCheckbox = bulkResultsSection?.querySelector("[data-obus-bulk-select-all='1']");
    const bulkTotalCounter = bulkResultsSection?.querySelector("[data-obus-bulk-check-total='1']");
    const bulkMissingCounter = bulkResultsSection?.querySelector("[data-obus-bulk-check-missing='1']");
    const bulkExistingCounter = bulkResultsSection?.querySelector("[data-obus-bulk-check-existing='1']");
    const bulkErrorCounter = bulkResultsSection?.querySelector("[data-obus-bulk-check-error='1']");
    const bulkTemplateModal = document.querySelector("#obus-bulk-template-modal");
    const bulkTemplateCloseBtn = bulkTemplateModal?.querySelector("[data-obus-bulk-template-close='1']");
    const bulkTemplateForm = bulkTemplateModal?.querySelector("[data-obus-bulk-template-form='1']");
    const bulkTemplateNameInput = bulkTemplateModal?.querySelector("[data-obus-bulk-template-name='1']");
    const bulkTemplateStatus = bulkTemplateModal?.querySelector("[data-obus-bulk-template-status='1']");
    const bulkTemplateList = bulkTemplateModal?.querySelector("[data-obus-bulk-template-list='1']");
    const liveRowByKey = new Map();
    const bulkCheckItemByKey = new Map();
    const bulkCheckRowByKey = new Map();
    const bulkSelectedCreateKeys = new Set();
    let bulkTemplates = [];
    let bulkCheckItems = [];
    let bulkRowCounter = 1;
    let bulkTemplatesLoading = false;
    let legacyBulkTemplatesImportAttempted = false;
    let typeAheadText = "";
    let typeAheadTimerId = null;

    const parseJson = (raw, fallback) => {
      const text = String(raw || "").trim();
      if (!text) return fallback;
      try {
        return JSON.parse(text);
      } catch (err) {
        return fallback;
      }
    };
    const wait = (ms) =>
      new Promise((resolve) => {
        window.setTimeout(resolve, Math.max(0, Number(ms) || 0));
      });
    const formatTemplateTimestamp = (timestamp) => {
      const value = Number(timestamp || 0);
      if (!Number.isFinite(value) || value <= 0) return "";
      try {
        return new Intl.DateTimeFormat("tr-TR", {
          dateStyle: "short",
          timeStyle: "short"
        }).format(new Date(value));
      } catch (err) {
        return "";
      }
    };
    const setLiveMessage = (text = "", kind = "") => {
      if (!liveMessage) return;
      liveMessage.textContent = String(text || "").trim();
      liveMessage.hidden = !liveMessage.textContent;
      liveMessage.className = `obus-live-message ${kind === "success" ? "inline-success" : "inline-alert"}`.trim();
    };
    const setBulkTemplateStatus = (text = "", kind = "") => {
      if (!bulkTemplateStatus) return;
      bulkTemplateStatus.textContent = String(text || "").trim();
      bulkTemplateStatus.hidden = !bulkTemplateStatus.textContent;
      bulkTemplateStatus.className = `form-feedback${kind ? ` ${kind}` : ""}`;
    };
    const setLiveCounters = ({ processed = 0, total = 0, success = 0, failure = 0 } = {}) => {
      if (liveProgress) {
        liveProgress.textContent = `İşlenen: ${processed}/${total}`;
      }
      if (liveSuccess) {
        liveSuccess.textContent = `Başarılı: ${success}`;
      }
      if (liveFailure) {
        liveFailure.textContent = `Hatalı: ${failure}`;
      }
    };
    const getBulkRows = () =>
      bulkUserList ? Array.from(bulkUserList.querySelectorAll("[data-obus-bulk-user-row='1']")) : [];
    const normalizeBulkTemplateEntries = (entries) =>
      (Array.isArray(entries) ? entries : [])
        .map((entry) => ({
          fullName: String(entry?.fullName || "").trim(),
          username: String(entry?.username || "").trim(),
          password: String(entry?.password || "")
        }))
        .filter((entry) => entry.fullName || entry.username || entry.password);
    const isBulkMissingCandidate = (item) =>
      item &&
      item.exists === false &&
      !String(item.error || "").trim() &&
      String(item.liveStatusKind || "").trim() !== "success" &&
      String(item.liveStatusKind || "").trim() !== "queued";
    const syncBulkCheckItemMap = () => {
      bulkCheckItemByKey.clear();
      bulkCheckItems.forEach((item) => {
        const key = String(item?.key || "").trim();
        if (key) bulkCheckItemByKey.set(key, item);
      });
    };
    const setBulkCheckItems = (items) => {
      bulkCheckItems = Array.isArray(items) ? items : [];
      syncBulkCheckItemMap();
    };
    const getBulkCheckStatusState = (item) => {
      const liveStatusText = String(item?.liveStatusText || "").trim();
      const liveStatusKind = String(item?.liveStatusKind || "").trim();
      const liveStatusDetail = String(item?.liveStatusDetail || "").trim();
      if (liveStatusText) {
        return {
          text: liveStatusText,
          kind: liveStatusKind || "pending",
          detail: liveStatusDetail
        };
      }

      const queryError = String(item?.error || "").trim();
      if (queryError) {
        return {
          text: queryError,
          kind: "failure",
          detail: queryError
        };
      }

      if (item?.exists === true) {
        return {
          text: "Var",
          kind: "success",
          detail: ""
        };
      }

      return {
        text: "Yok",
        kind: "pending",
        detail: ""
      };
    };
    const updateBulkCounters = () => {
      if (!isBulkMode) return;
      const total = bulkCheckItems.length;
      const missing = bulkCheckItems.filter((item) => item.exists === false).length;
      const existing = bulkCheckItems.filter((item) => item.exists === true).length;
      const errors = bulkCheckItems.filter((item) => item.exists === null || item.error).length;
      if (bulkTotalCounter) bulkTotalCounter.textContent = `Toplam: ${total}`;
      if (bulkMissingCounter) bulkMissingCounter.textContent = `Yok: ${missing}`;
      if (bulkExistingCounter) bulkExistingCounter.textContent = `Var: ${existing}`;
      if (bulkErrorCounter) bulkErrorCounter.textContent = `Hata: ${errors}`;
    };
    const updateBulkCreateButtonState = () => {
      if (!bulkCreateBtn) return;
      const selectedCount = bulkCheckItems.filter((item) => {
        const key = String(item?.key || "").trim();
        return key && bulkSelectedCreateKeys.has(key) && isBulkMissingCandidate(item);
      }).length;
      bulkCreateBtn.disabled = selectedCount === 0;
    };
    const updateBulkSelectAllState = () => {
      if (!bulkSelectAllCheckbox) return;
      const missingKeys = bulkCheckItems.filter((item) => isBulkMissingCandidate(item)).map((item) => String(item.key || ""));
      if (missingKeys.length === 0) {
        bulkSelectAllCheckbox.checked = false;
        bulkSelectAllCheckbox.disabled = true;
        return;
      }
      bulkSelectAllCheckbox.disabled = false;
      bulkSelectAllCheckbox.checked = missingKeys.every((key) => bulkSelectedCreateKeys.has(key));
    };
    const clearBulkCheckState = () => {
      if (!isBulkMode) return;
      setBulkCheckItems([]);
      bulkCheckRowByKey.clear();
      bulkSelectedCreateKeys.clear();
      if (bulkResultsBody) {
        bulkResultsBody.innerHTML = "";
      }
      if (bulkResultsSection) {
        bulkResultsSection.hidden = true;
      }
      updateBulkCounters();
      updateBulkSelectAllState();
      updateBulkCreateButtonState();
    };
    const readBulkUserEntries = () =>
      normalizeBulkTemplateEntries(
        getBulkRows().map((row, index) => {
          const entryId =
            String(row.getAttribute("data-obus-bulk-entry-id") || `row-${index + 1}`).trim() || `row-${index + 1}`;
          const fullName = String(row.querySelector("[data-obus-bulk-user-full-name='1']")?.value || "").trim();
          const username = String(row.querySelector("[data-obus-bulk-user-username='1']")?.value || "").trim();
          const password = String(row.querySelector("[data-obus-bulk-user-password='1']")?.value || "");
          return { entryId, fullName, username, password };
        })
      );
    const bindBulkUserRow = (row) => {
      if (!row || row.dataset.bulkRowBound === "1") return;
      row.dataset.bulkRowBound = "1";
      if (!String(row.getAttribute("data-obus-bulk-entry-id") || "").trim()) {
        bulkRowCounter += 1;
        row.setAttribute("data-obus-bulk-entry-id", `row-${bulkRowCounter}`);
      }
      [
        row.querySelector("[data-obus-bulk-user-full-name='1']"),
        row.querySelector("[data-obus-bulk-user-username='1']"),
        row.querySelector("[data-obus-bulk-user-password='1']")
      ]
        .filter(Boolean)
        .forEach((inputEl) => {
          inputEl.addEventListener("input", () => {
            clearBulkCheckState();
          });
        });
      const removeBtn = row.querySelector("[data-obus-bulk-remove-row='1']");
      if (!removeBtn) return;
      removeBtn.addEventListener("click", () => {
        const rows = getBulkRows();
        if (rows.length <= 1) {
          const fullNameEl = row.querySelector("[data-obus-bulk-user-full-name='1']");
          const usernameEl = row.querySelector("[data-obus-bulk-user-username='1']");
          const passwordEl = row.querySelector("[data-obus-bulk-user-password='1']");
          if (fullNameEl) fullNameEl.value = "";
          if (usernameEl) usernameEl.value = "";
          if (passwordEl) passwordEl.value = "";
          clearBulkCheckState();
          return;
        }
        row.remove();
        clearBulkCheckState();
      });
    };
    const addBulkUserRow = (entry = {}) => {
      if (!bulkUserList) return;
      bulkRowCounter += 1;
      const row = document.createElement("div");
      row.className = "obus-bulk-user-row";
      row.setAttribute("data-obus-bulk-user-row", "1");
      row.setAttribute("data-obus-bulk-entry-id", `row-${bulkRowCounter}`);
      row.innerHTML = `
        <input type="text" placeholder="Ad Soyad" data-obus-bulk-user-full-name="1" autocomplete="off" />
        <input type="text" placeholder="KullanıcıAdı" data-obus-bulk-user-username="1" autocomplete="off" />
        <input type="password" placeholder="Şifre" data-obus-bulk-user-password="1" autocomplete="new-password" />
        <button type="button" class="ghost" data-obus-bulk-remove-row="1">Sil</button>
      `;
      const fullNameEl = row.querySelector("[data-obus-bulk-user-full-name='1']");
      const usernameEl = row.querySelector("[data-obus-bulk-user-username='1']");
      const passwordEl = row.querySelector("[data-obus-bulk-user-password='1']");
      if (fullNameEl) fullNameEl.value = String(entry.fullName || "").trim();
      if (usernameEl) usernameEl.value = String(entry.username || "").trim();
      if (passwordEl) passwordEl.value = String(entry.password || "");
      bulkUserList.appendChild(row);
      bindBulkUserRow(row);
    };
    const replaceBulkUserRows = (entries) => {
      if (!bulkUserList) return;
      const normalizedEntries = normalizeBulkTemplateEntries(entries);
      bulkUserList.innerHTML = "";
      bulkRowCounter = 0;
      if (normalizedEntries.length === 0) {
        addBulkUserRow();
      } else {
        normalizedEntries.forEach((entry) => {
          addBulkUserRow(entry);
        });
      }
      clearBulkCheckState();
    };
    const renderBulkTemplateList = () => {
      if (!bulkTemplateList) return;
      bulkTemplateList.innerHTML = "";
      if (bulkTemplatesLoading) {
        const loading = document.createElement("div");
        loading.className = "obus-bulk-template-empty";
        loading.textContent = "Şablonlar yükleniyor...";
        bulkTemplateList.appendChild(loading);
        return;
      }
      if (!bulkTemplates.length) {
        const empty = document.createElement("div");
        empty.className = "obus-bulk-template-empty";
        empty.textContent = "Henüz kayıtlı bir şablon yok.";
        bulkTemplateList.appendChild(empty);
        return;
      }

      bulkTemplates.forEach((template) => {
        const item = document.createElement("div");
        item.className = "obus-bulk-template-item";

        const content = document.createElement("div");
        const name = document.createElement("div");
        name.className = "obus-bulk-template-name";
        name.textContent = template.name;

        const meta = document.createElement("div");
        meta.className = "obus-bulk-template-meta";
        const rowCount = Array.isArray(template.entries) ? template.entries.length : 0;
        const updatedText = formatTemplateTimestamp(template.updatedAt || template.createdAt);
        const updatedBy = String(template.updatedByName || template.createdByName || "").trim();
        const rowCountMeta = document.createElement("span");
        rowCountMeta.textContent = `${rowCount} satır`;
        meta.appendChild(rowCountMeta);
        if (updatedText) {
          const updatedMeta = document.createElement("span");
          updatedMeta.textContent = `Son güncelleme: ${updatedText}`;
          meta.appendChild(updatedMeta);
        }
        if (updatedBy) {
          const updatedByMeta = document.createElement("span");
          updatedByMeta.textContent = `Ekleyen/Güncelleyen: ${updatedBy}`;
          meta.appendChild(updatedByMeta);
        }

        const preview = document.createElement("div");
        preview.className = "obus-bulk-template-preview";
        preview.textContent = (template.entries || [])
          .slice(0, 2)
          .map((entry) => {
            const nameText = String(entry?.fullName || "").trim();
            const usernameText = String(entry?.username || "").trim();
            return [nameText, usernameText ? `@${usernameText}` : ""].filter(Boolean).join(" ");
          })
          .filter(Boolean)
          .join(" | ");

        content.appendChild(name);
        content.appendChild(meta);
        if (preview.textContent) {
          content.appendChild(preview);
        }

        const loadBtn = document.createElement("button");
        loadBtn.type = "button";
        loadBtn.className = "button-link";
        loadBtn.textContent = "Yükle";
        loadBtn.addEventListener("click", () => {
          replaceBulkUserRows(template.entries || []);
          closeBulkTemplateModal();
          setLiveMessage(`'${template.name}' şablonu yüklendi.`, "success");
        });

        item.appendChild(content);
        item.appendChild(loadBtn);
        bulkTemplateList.appendChild(item);
      });
    };
    const upsertBulkTemplate = (item) => {
      const normalizedItem = normalizeObusBulkUserTemplateItems([item])[0];
      if (!normalizedItem) return;
      const existingIndex = bulkTemplates.findIndex((template) => template.id === normalizedItem.id);
      if (existingIndex >= 0) {
        bulkTemplates.splice(existingIndex, 1, normalizedItem);
      } else {
        bulkTemplates.unshift(normalizedItem);
      }
      bulkTemplates.sort((a, b) => Number(b.updatedAt || 0) - Number(a.updatedAt || 0));
    };
    const importLegacyBulkTemplatesIfNeeded = async ({ showStatus = false } = {}) => {
      if (legacyBulkTemplatesImportAttempted) return { importedCount: 0, failedCount: 0, skippedCount: 0 };
      legacyBulkTemplatesImportAttempted = true;

      const legacyTemplates = loadLegacyObusBulkUserTemplatesFromStorage();
      if (!legacyTemplates.length) {
        return { importedCount: 0, failedCount: 0, skippedCount: 0 };
      }

      const existingNameSet = new Set(
        bulkTemplates.map((template) => String(template?.name || "").trim().toLocaleLowerCase("tr")).filter(Boolean)
      );
      let importedCount = 0;
      let failedCount = 0;
      let skippedCount = 0;

      for (const legacyTemplate of legacyTemplates) {
        const normalizedName = String(legacyTemplate?.name || "").trim();
        const nameKey = normalizedName.toLocaleLowerCase("tr");
        if (!normalizedName || existingNameSet.has(nameKey)) {
          skippedCount += 1;
          continue;
        }

        const saveResult = await saveObusBulkUserTemplate(bulkTemplatesApiUrl, {
          name: normalizedName,
          entries: Array.isArray(legacyTemplate?.entries) ? legacyTemplate.entries : []
        });
        if (saveResult.error || !saveResult.item) {
          failedCount += 1;
          continue;
        }

        upsertBulkTemplate(saveResult.item);
        existingNameSet.add(nameKey);
        importedCount += 1;
      }

      if (failedCount === 0) {
        clearLegacyObusBulkUserTemplatesFromStorage();
      }
      renderBulkTemplateList();
      if (showStatus && importedCount > 0) {
        setBulkTemplateStatus(`${importedCount} eski tarayıcı şablonu SQL'e aktarıldı.`, "success");
      } else if (showStatus && failedCount > 0) {
        setBulkTemplateStatus(
          `${importedCount} şablon SQL'e aktarıldı, ${failedCount} şablon aktarılırken hata oluştu.`,
          "error"
        );
      }
      return { importedCount, failedCount, skippedCount };
    };
    const refreshBulkTemplates = async ({ showStatus = false } = {}) => {
      bulkTemplatesLoading = true;
      renderBulkTemplateList();
      if (showStatus) {
        setBulkTemplateStatus("Şablonlar yükleniyor...", "");
      }

      const result = await loadObusBulkUserTemplates(bulkTemplatesApiUrl);
      bulkTemplatesLoading = false;
      if (result.error) {
        renderBulkTemplateList();
        if (showStatus) {
          setBulkTemplateStatus(result.error, "error");
        }
        return false;
      }

      bulkTemplates = Array.isArray(result.items) ? result.items : [];
      renderBulkTemplateList();
      const importResult = await importLegacyBulkTemplatesIfNeeded({ showStatus });
      if (showStatus && Number(importResult.importedCount || 0) === 0 && Number(importResult.failedCount || 0) === 0) {
        setBulkTemplateStatus("", "");
      }
      return true;
    };
    const openBulkTemplateModal = async (mode = "load") => {
      if (!bulkTemplateModal) return;
      setBulkTemplateStatus("", "");
      bulkTemplateModal.classList.add("active");
      bulkTemplateModal.setAttribute("aria-hidden", "false");
      renderBulkTemplateList();
      await refreshBulkTemplates({ showStatus: true });
      if (mode === "save") {
        const defaultName = String(bulkTemplateNameInput?.value || "").trim();
        if (bulkTemplateNameInput && !defaultName) {
          bulkTemplateNameInput.value = "";
        }
        bulkTemplateNameInput?.focus();
      } else {
        const firstButton = bulkTemplateList?.querySelector("button");
        if (firstButton instanceof HTMLButtonElement) {
          firstButton.focus();
        } else {
          bulkTemplateNameInput?.focus();
        }
      }
    };
    const closeBulkTemplateModal = () => {
      if (!bulkTemplateModal) return;
      bulkTemplateModal.classList.remove("active");
      bulkTemplateModal.setAttribute("aria-hidden", "true");
      setBulkTemplateStatus("", "");
    };
    const renderBulkCheckRows = () => {
      if (!bulkResultsBody) return;
      const filterValue = String(bulkFilterSelect?.value || "all").trim();
      bulkResultsBody.innerHTML = "";
      bulkCheckRowByKey.clear();

      bulkCheckItems.forEach((item) => {
        const exists = item.exists === true;
        const missing = item.exists === false;
        if (filterValue === "missing" && !missing) return;
        if (filterValue === "existing" && !exists) return;

        const rowKey = String(item.key || "").trim();
        const row = document.createElement("tr");
        row.setAttribute("data-obus-bulk-check-key", rowKey);
        const canCreate = isBulkMissingCandidate(item);
        const statusState = getBulkCheckStatusState(item);

        const checkboxCell = document.createElement("td");
        const checkbox = document.createElement("input");
        checkbox.type = "checkbox";
        checkbox.setAttribute("data-obus-bulk-row-select", "1");
        checkbox.checked = canCreate && bulkSelectedCreateKeys.has(rowKey);
        checkbox.disabled = !canCreate;
        checkbox.addEventListener("change", () => {
          if (!rowKey) return;
          if (checkbox.checked) {
            bulkSelectedCreateKeys.add(rowKey);
          } else {
            bulkSelectedCreateKeys.delete(rowKey);
          }
          updateBulkSelectAllState();
          updateBulkCreateButtonState();
        });
        checkboxCell.appendChild(checkbox);

        const companyCell = document.createElement("td");
        companyCell.textContent = String(item.companyLabel || item.companyValue || "Firma");
        const usernameCell = document.createElement("td");
        usernameCell.textContent = String(item.username || "");
        const fullNameCell = document.createElement("td");
        fullNameCell.textContent = String(item.fullName || "");
        const statusCell = document.createElement("td");
        statusCell.setAttribute("data-obus-create-status", "1");
        statusCell.className = "obus-live-status pending";
        statusCell.textContent = "-";

        row.appendChild(checkboxCell);
        row.appendChild(companyCell);
        row.appendChild(usernameCell);
        row.appendChild(fullNameCell);
        row.appendChild(statusCell);
        bulkResultsBody.appendChild(row);
        if (rowKey) {
          bulkCheckRowByKey.set(rowKey, row);
        }
        setCreateRowStatus(row, statusState.text, statusState.kind, statusState.detail);
      });

      if (bulkResultsSection) {
        bulkResultsSection.hidden = bulkCheckItems.length === 0;
      }
      updateBulkCounters();
      updateBulkSelectAllState();
      updateBulkCreateButtonState();
    };
    const setCreateRowStatus = (row, text, kind, detail = "") => {
      const cell = row?.querySelector("[data-obus-create-status='1']");
      if (!cell) return;
      cell.textContent = String(text || "").trim() || "-";
      cell.className = `obus-live-status ${kind || "pending"}`;
      const tooltip = String(detail || "").trim();
      if (tooltip) {
        cell.setAttribute("title", tooltip);
        cell.classList.add("has-detail");
      } else {
        cell.removeAttribute("title");
        cell.classList.remove("has-detail");
      }
    };
    const updateBulkCheckRowStatusByKey = (key) => {
      const normalizedKey = String(key || "").trim();
      if (!normalizedKey) return;
      const row = bulkCheckRowByKey.get(normalizedKey);
      const item = bulkCheckItemByKey.get(normalizedKey);
      if (!row || !item) return;
      const checkbox = row.querySelector("[data-obus-bulk-row-select='1']");
      if (checkbox instanceof HTMLInputElement) {
        const canCreate = isBulkMissingCandidate(item);
        checkbox.disabled = !canCreate;
        checkbox.checked = canCreate && bulkSelectedCreateKeys.has(normalizedKey);
      }
      const statusState = getBulkCheckStatusState(item);
      setCreateRowStatus(row, statusState.text, statusState.kind, statusState.detail);
    };
    const setBulkCheckItemLiveState = (key, { text = "", kind = "", detail = "" } = {}) => {
      const normalizedKey = String(key || "").trim();
      if (!normalizedKey) return;
      const item = bulkCheckItemByKey.get(normalizedKey);
      if (!item) return;

      item.liveStatusText = String(text || "").trim();
      item.liveStatusKind = String(kind || "").trim();
      item.liveStatusDetail = String(detail || "").trim();
      if (item.liveStatusKind === "success") {
        bulkSelectedCreateKeys.delete(normalizedKey);
      }
      updateBulkCheckRowStatusByKey(normalizedKey);
      updateBulkSelectAllState();
      updateBulkCreateButtonState();
    };
    const markBulkCreateTargetsQueued = (items) => {
      (Array.isArray(items) ? items : []).forEach((item) => {
        const key = String(item?.key || "").trim();
        if (!key) return;
        setBulkCheckItemLiveState(key, {
          text: "Sırada",
          kind: "queued",
          detail: ""
        });
      });
    };
    const markBulkCreateTargetsFailedIfPending = (items, errorText) => {
      const fallbackError = String(errorText || "").trim() || "İşlem tamamlanamadı.";
      (Array.isArray(items) ? items : []).forEach((item) => {
        const key = String(item?.key || "").trim();
        const currentItem = bulkCheckItemByKey.get(key);
        const currentKind = String(currentItem?.liveStatusKind || "").trim();
        if (!key || currentKind === "success" || currentKind === "failure") return;
        setBulkCheckItemLiveState(key, {
          text: fallbackError,
          kind: "failure",
          detail: fallbackError
        });
      });
    };
    const renderCreateLiveRows = (items) => {
      liveRowByKey.clear();
      if (!liveBody) return;
      liveBody.innerHTML = "";
      (Array.isArray(items) ? items : []).forEach((item) => {
        const key = String(item?.key || "").trim();
        const label = String(item?.label || key || "Firma").trim();
        if (!key) return;
        const row = document.createElement("tr");
        row.setAttribute("data-obus-create-row-key", key);
        const labelCell = document.createElement("td");
        labelCell.textContent = label;
        const statusCell = document.createElement("td");
        statusCell.setAttribute("data-obus-create-status", "1");
        statusCell.className = "obus-live-status pending";
        statusCell.textContent = "Sırada";
        row.appendChild(labelCell);
        row.appendChild(statusCell);
        liveBody.appendChild(row);
        liveRowByKey.set(key, row);
      });
      if (liveResultsSection) {
        liveResultsSection.hidden = liveRowByKey.size === 0;
      }
    };
    const applyCreateEventToRow = (eventItem) => {
      const key = String(eventItem?.key || "").trim();
      if (isBulkMode) {
        if (!key) return;
        if (eventItem?.ok === true) {
          setBulkCheckItemLiveState(key, {
            text: "OK",
            kind: "success",
            detail: String(eventItem?.message || "Kullanıcı oluşturuldu.").trim()
          });
        } else {
          const errorText = String(eventItem?.error || "İşlem başarısız").trim();
          const errorDetail = String(eventItem?.errorDetail || errorText).trim();
          setBulkCheckItemLiveState(key, {
            text: errorText,
            kind: "failure",
            detail: errorDetail
          });
        }
        return;
      }
      const row = liveRowByKey.get(key);
      if (!row) return;
      if (eventItem?.ok === true) {
        setCreateRowStatus(row, eventItem.message || "Kullanıcı oluşturuldu", "success");
      } else {
        setCreateRowStatus(
          row,
          eventItem.error || "İşlem başarısız",
          "failure",
          eventItem.errorDetail || ""
        );
      }
    };
    const pollLiveJob = async (jobId, onEvent) => {
      let cursor = 0;
      while (true) {
        if (!document.body.contains(form)) {
          throw new Error("Sayfa değiştiği için canlı takip durduruldu.");
        }
        const response = await fetch(`/api/obus-live/${encodeURIComponent(jobId)}?cursor=${cursor}`, {
          headers: { Accept: "application/json" }
        });
        const data = await parseJsonResponse(response);
        if (!response.ok || !data?.ok) {
          throw new Error(getApiErrorMessage(response, data, "Canlı işlem durumu okunamadı"));
        }

        const events = Array.isArray(data.events) ? data.events : [];
        events.forEach((eventItem) => {
          if (typeof onEvent === "function") onEvent(eventItem);
        });
        cursor = Number.isFinite(Number(data.cursor)) ? Number(data.cursor) : cursor;
        setLiveCounters({
          processed: Number(data.processedCount || 0),
          total: Number(data.totalCount || 0),
          success: Number(data.successCount || 0),
          failure: Number(data.failureCount || 0)
        });

        if (data.done) return data;
        await wait(450);
      }
    };

    const readSelectedCompanyValues = () =>
      companyCheckboxes
        .filter((item) => item.checked)
        .map((item) => String(item.value || "").trim())
        .filter(Boolean);

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

    const findMatchingCompanyOption = (queryText) => {
      const normalizedQuery = normalizeSearchText(queryText);
      if (!normalizedQuery) return null;
      const optionRows = Array.from(form.querySelectorAll(".obus-company-option[data-company-option-row='1']"));
      return (
        optionRows.find((row) => {
          const labelText = String(row.querySelector("span")?.textContent || "");
          return normalizeSearchText(labelText).includes(normalizedQuery);
        }) || null
      );
    };

    const focusCompanyOptionRow = (row) => {
      if (!row) return;
      row.scrollIntoView({ block: "nearest" });
      row.classList.add("obus-company-option-focus");
      setTimeout(() => {
        row.classList.remove("obus-company-option-focus");
      }, 450);
    };

    const handleTypeAheadKey = (event) => {
      if (event.metaKey || event.ctrlKey || event.altKey) return;
      if (event.key === "Escape") return;
      if (event.key === "Tab") return;

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

      if (dropdown && dropdown.hidden) {
        openDropdown();
      }
      focusCompanyOptionRow(findMatchingCompanyOption(typeAheadText));
    };

    const updateCompanyTriggerLabel = () => {
      if (!trigger) return;
      const selectedValues = readSelectedCompanyValues();
      const totalCount = companyCheckboxes.length;
      if (selectedValues.length === 0) {
        trigger.textContent = "Firma seçiniz";
        return;
      }
      if (selectedValues.length === totalCount) {
        trigger.textContent = "Hepsi";
        return;
      }
      if (selectedValues.length === 1) {
        const selectedItem = companyCheckboxes.find(
          (item) => item.checked && String(item.value || "").trim() === selectedValues[0]
        );
        const labelText = String(selectedItem?.closest("label")?.querySelector("span")?.textContent || "").trim();
        trigger.textContent = labelText || "1 firma seçildi";
        return;
      }
      trigger.textContent = `${selectedValues.length} firma seçildi`;
    };

    const syncSelectedCompanies = () => {
      const selectedValues = readSelectedCompanyValues();
      if (selectAllCheckbox) {
        selectAllCheckbox.checked = selectedValues.length > 0 && selectedValues.length === companyCheckboxes.length;
      }
      if (selectedCompaniesInput) {
        selectedCompaniesInput.value = JSON.stringify(selectedValues);
      }
      updateCompanyTriggerLabel();
    };

    const applyInitialCompanySelection = () => {
      const parsed = parseJson(selectedCompaniesInput?.value, []);
      const initialValues = Array.isArray(parsed)
        ? parsed.map((item) => String(item || "").trim()).filter(Boolean)
        : [];
      const allowed = new Set(
        companyCheckboxes.map((item) => String(item.value || "").trim()).filter(Boolean)
      );
      const normalizedInitial = initialValues.filter((value) => allowed.has(value));
      const selectedSet =
        normalizedInitial.length > 0
          ? new Set(normalizedInitial)
          : new Set(companyCheckboxes.map((item) => String(item.value || "").trim()).filter(Boolean));

      companyCheckboxes.forEach((item) => {
        item.checked = selectedSet.has(String(item.value || "").trim());
      });
      syncSelectedCompanies();
    };

    const closeDropdown = () => {
      if (!dropdown || !trigger) return;
      dropdown.hidden = true;
      trigger.setAttribute("aria-expanded", "false");
      clearTypeAhead();
    };

    const openDropdown = () => {
      if (!dropdown || !trigger) return;
      dropdown.hidden = false;
      trigger.setAttribute("aria-expanded", "true");
    };

    if (trigger && dropdown) {
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
        if (!multiselect) return;
        if (multiselect.contains(event.target)) return;
        closeDropdown();
      });

      document.addEventListener("keydown", (event) => {
        if (event.key === "Escape") {
          closeDropdown();
        }
      });
    }

    if (multiselect) {
      multiselect.addEventListener("keydown", (event) => {
        const activeElement = document.activeElement;
        const isCheckboxActive =
          activeElement instanceof HTMLInputElement && activeElement.type === "checkbox";
        if (!isCheckboxActive) return;
        handleTypeAheadKey(event);
      });
    }

    if (selectAllCheckbox) {
      selectAllCheckbox.addEventListener("change", () => {
        companyCheckboxes.forEach((item) => {
          item.checked = selectAllCheckbox.checked;
        });
        syncSelectedCompanies();
        if (isBulkMode) {
          clearBulkCheckState();
        }
      });
    }

    companyCheckboxes.forEach((item) => {
      item.addEventListener("change", () => {
        syncSelectedCompanies();
        if (isBulkMode) {
          clearBulkCheckState();
        }
      });
    });

    if (isBulkMode) {
      const initialRows = getBulkRows();
      bulkRowCounter = Math.max(1, initialRows.length);
      initialRows.forEach((row, index) => {
        if (!String(row.getAttribute("data-obus-bulk-entry-id") || "").trim()) {
          row.setAttribute("data-obus-bulk-entry-id", `row-${index + 1}`);
        }
        bindBulkUserRow(row);
      });

      if (bulkAddRowBtn) {
        bulkAddRowBtn.addEventListener("click", () => {
          addBulkUserRow();
          clearBulkCheckState();
        });
      }

      bulkTemplateSaveOpenBtn?.addEventListener("click", () => {
        void openBulkTemplateModal("save");
      });

      bulkTemplateLoadOpenBtn?.addEventListener("click", () => {
        void openBulkTemplateModal("load");
      });

      bulkTemplateCloseBtn?.addEventListener("click", () => {
        closeBulkTemplateModal();
      });

      bulkTemplateModal?.addEventListener("click", (event) => {
        if (event.target === bulkTemplateModal) {
          closeBulkTemplateModal();
        }
      });

      bulkTemplateForm?.addEventListener("submit", async (event) => {
        event.preventDefault();
        const name = String(bulkTemplateNameInput?.value || "").trim();
        if (!name) {
          setBulkTemplateStatus("Şablon adı zorunludur.", "error");
          bulkTemplateNameInput?.focus();
          return;
        }

        const entries = readBulkUserEntries();
        if (entries.length === 0) {
          setBulkTemplateStatus("Kaydetmek için en az bir dolu kullanıcı satırı girin.", "error");
          return;
        }

        const existingIndex = bulkTemplates.findIndex(
          (item) => String(item.name || "").trim().toLocaleLowerCase("tr") === name.toLocaleLowerCase("tr")
        );
        if (existingIndex >= 0) {
          const shouldOverwrite = window.confirm(`'${name}' adlı şablon var. Üzerine yazılsın mı?`);
          if (!shouldOverwrite) return;
        }

        setBulkTemplateStatus("Şablon kaydediliyor...", "");
        const saveResult = await saveObusBulkUserTemplate(bulkTemplatesApiUrl, {
          name,
          entries
        });
        if (saveResult.error || !saveResult.item) {
          setBulkTemplateStatus(saveResult.error || "Şablon kaydedilemedi.", "error");
          return;
        }

        upsertBulkTemplate(saveResult.item);
        renderBulkTemplateList();
        setBulkTemplateStatus(`'${name}' şablonu kaydedildi.`, "success");
      });

      if (bulkFilterSelect) {
        bulkFilterSelect.addEventListener("change", () => {
          renderBulkCheckRows();
        });
      }

      if (bulkSelectAllCheckbox) {
        bulkSelectAllCheckbox.addEventListener("change", () => {
          const missingKeys = bulkCheckItems
            .filter((item) => isBulkMissingCandidate(item))
            .map((item) => String(item.key || ""));
          bulkSelectedCreateKeys.clear();
          if (bulkSelectAllCheckbox.checked) {
            missingKeys.forEach((key) => {
              if (key) bulkSelectedCreateKeys.add(key);
            });
          }
          renderBulkCheckRows();
        });
      }

      if (bulkCreateBtn) {
        const defaultCreateLabel = String(bulkCreateBtn.textContent || "").trim() || "Yok Olanları Oluştur";
        bulkCreateBtn.disabled = true;
        bulkCreateBtn.textContent = defaultCreateLabel;
        bulkCreateBtn.addEventListener("click", async () => {
          if (form.dataset.liveSubmitting === "1") return;
          form.dataset.liveSubmitting = "1";
          setLiveMessage("", "");
          syncSelectedCompanies();

          const selectedValues = readSelectedCompanyValues();
          if (selectedValues.length === 0) {
            setLiveMessage("En az bir firma seçmelisiniz.", "error");
            form.dataset.liveSubmitting = "0";
            return;
          }

          const selectedCompanySet = new Set(selectedValues.map((item) => String(item || "").trim()).filter(Boolean));
          const selectedTargets = bulkCheckItems.filter((item) => {
            const key = String(item?.key || "");
            const companyValue = String(item?.companyValue || "").trim();
            return (
              key &&
              companyValue &&
              selectedCompanySet.has(companyValue) &&
              bulkSelectedCreateKeys.has(key) &&
              isBulkMissingCandidate(item)
            );
          });
          if (selectedTargets.length === 0) {
            setLiveMessage(
              "Önce sorgu sonucu listesinde yok olan en az bir kullanıcı seçmelisiniz. Firma seçimi değiştiyse yeniden sorgulayın.",
              "error"
            );
            form.dataset.liveSubmitting = "0";
            return;
          }

          const defaultCheckLabel = String(submitBtn?.textContent || "").trim() || "Kullanıcıları Sorgula";
          if (submitBtn) {
            submitBtn.disabled = true;
            submitBtn.textContent = "Bekleyin...";
          }
          bulkCreateBtn.disabled = true;
          bulkCreateBtn.textContent = "Oluşturuluyor...";
          form.classList.add("is-loading");
          if (loadingMessage) loadingMessage.hidden = false;

          try {
            const startResponse = await fetch(liveStartUrl, {
              method: "POST",
              headers: {
                "Content-Type": "application/json",
                Accept: "application/json"
              },
              body: JSON.stringify({
                selectedCompanies: selectedValues,
                selectedKeys: selectedTargets
                  .map((item) => String(item?.key || "").trim())
                  .filter(Boolean),
                userEntries: readBulkUserEntries(),
                bulk: String(bulkInput?.value || "1").trim()
              })
            });
            const startData = await parseJsonResponse(startResponse);
            if (!startResponse.ok || !startData?.ok) {
              throw new Error(getApiErrorMessage(startResponse, startData, "Toplu kullanıcı oluşturma başlatılamadı"));
            }

            const items = Array.isArray(startData.items) ? startData.items : [];
            markBulkCreateTargetsQueued(selectedTargets);
            renderCreateLiveRows(items);
            setLiveCounters({
              processed: 0,
              total: Number(startData.totalCount || items.length || 0),
              success: 0,
              failure: 0
            });

            const finalState = await pollLiveJob(startData.jobId, applyCreateEventToRow);
            if (finalState?.error) {
              markBulkCreateTargetsFailedIfPending(selectedTargets, finalState.error);
              setLiveMessage(finalState.error, "error");
            } else if (Number(finalState?.failureCount || 0) > 0) {
              setLiveMessage("Toplu oluşturma tamamlandı. Bazı kayıtlar oluşturulamadı.", "error");
            } else {
              setLiveMessage("Toplu oluşturma tamamlandı. Seçili yok kullanıcılar oluşturuldu.", "success");
            }
          } catch (err) {
            markBulkCreateTargetsFailedIfPending(
              selectedTargets,
              err?.message || "Toplu kullanıcı oluşturma başlatılamadı."
            );
            setLiveMessage(err?.message || "Toplu kullanıcı oluşturma başlatılamadı.", "error");
          } finally {
            form.dataset.liveSubmitting = "0";
            if (submitBtn) {
              submitBtn.disabled = false;
              submitBtn.textContent = defaultCheckLabel;
            }
            bulkCreateBtn.disabled = false;
            bulkCreateBtn.textContent = defaultCreateLabel;
            form.classList.remove("is-loading");
            if (loadingMessage) loadingMessage.hidden = true;
            updateBulkCreateButtonState();
          }
        });
      }
    }

    if (submitBtn) {
      const defaultLabel = String(submitBtn.textContent || "").trim() || "Kullanıcı Oluştur";
      form.classList.remove("is-loading");
      submitBtn.disabled = false;
      submitBtn.textContent = defaultLabel;
      if (loadingMessage) loadingMessage.hidden = true;

      form.addEventListener("submit", async (event) => {
        event.preventDefault();
        if (form.dataset.liveSubmitting === "1") return;
        form.dataset.liveSubmitting = "1";
        setLiveMessage("", "");
        syncSelectedCompanies();

        const selectedValues = readSelectedCompanyValues();
        if (selectedValues.length === 0) {
          setLiveMessage("En az bir firma seçmelisiniz.", "error");
          form.dataset.liveSubmitting = "0";
          return;
        }

        if (isBulkMode) {
          const userEntries = readBulkUserEntries();
          if (userEntries.length === 0) {
            setLiveMessage("En az bir kullanıcı satırı girmelisiniz.", "error");
            form.dataset.liveSubmitting = "0";
            return;
          }
          const hasMissingUsername = userEntries.some((entry) => !String(entry.username || "").trim());
          if (hasMissingUsername) {
            setLiveMessage("Sorgu için her satırda KullanıcıAdı zorunludur.", "error");
            form.dataset.liveSubmitting = "0";
            return;
          }

          clearBulkCheckState();
          submitBtn.disabled = true;
          submitBtn.textContent = "Sorgulanıyor...";
          if (bulkCreateBtn) bulkCreateBtn.disabled = true;
          form.classList.add("is-loading");
          if (loadingMessage) loadingMessage.hidden = false;

          try {
            const checkResponse = await fetch(bulkCheckUrl, {
              method: "POST",
              headers: {
                "Content-Type": "application/json",
                Accept: "application/json"
              },
              body: JSON.stringify({
                selectedCompanies: selectedValues,
                userEntries,
                bulk: String(bulkInput?.value || "1").trim()
              })
            });
            const checkData = await parseJsonResponse(checkResponse);
            if (!checkResponse.ok || !checkData?.ok) {
              throw new Error(getApiErrorMessage(checkResponse, checkData, "Toplu kullanıcı sorgusu başlatılamadı"));
            }

            setBulkCheckItems(Array.isArray(checkData.items) ? checkData.items : []);
            bulkSelectedCreateKeys.clear();
            bulkCheckItems.forEach((item) => {
              if (isBulkMissingCandidate(item)) {
                const key = String(item.key || "").trim();
                if (key) bulkSelectedCreateKeys.add(key);
              }
            });
            renderBulkCheckRows();

            const errorCount = Number(checkData.errorCount || 0);
            if (errorCount > 0) {
              setLiveMessage("Sorgu tamamlandı. Bazı satırlarda hata var; sadece Yok olanlar oluşturulabilir.", "error");
            } else {
              setLiveMessage("Sorgu tamamlandı. Var/Yok durumuna göre filtreleyip yok olanları oluşturabilirsiniz.", "success");
            }
          } catch (err) {
            setLiveMessage(err?.message || "Toplu kullanıcı sorgusu tamamlanamadı.", "error");
          } finally {
            form.dataset.liveSubmitting = "0";
            submitBtn.disabled = false;
            submitBtn.textContent = defaultLabel;
            form.classList.remove("is-loading");
            if (loadingMessage) loadingMessage.hidden = true;
            updateBulkCreateButtonState();
          }
          return;
        }

        submitBtn.disabled = true;
        submitBtn.textContent = "Oluşturuluyor...";
        form.classList.add("is-loading");
        if (loadingMessage) loadingMessage.hidden = false;

        try {
          const startResponse = await fetch(liveStartUrl, {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
              Accept: "application/json"
            },
            body: JSON.stringify({
              selectedCompanies: selectedValues,
              fullName: String(fullNameInput?.value || "").trim(),
              username: String(usernameInput?.value || "").trim(),
              password: String(passwordInput?.value || ""),
              bulk: String(bulkInput?.value || "0").trim()
            })
          });
          const startData = await parseJsonResponse(startResponse);
          if (!startResponse.ok || !startData?.ok) {
            throw new Error(getApiErrorMessage(startResponse, startData, "Kullanıcı oluşturma başlatılamadı"));
          }

          const items = Array.isArray(startData.items) ? startData.items : [];
          renderCreateLiveRows(items);
          setLiveCounters({
            processed: 0,
            total: Number(startData.totalCount || items.length || 0),
            success: 0,
            failure: 0
          });

          const finalState = await pollLiveJob(startData.jobId, applyCreateEventToRow);
          if (finalState?.error) {
            setLiveMessage(finalState.error, "error");
          } else if (Number(finalState?.failureCount || 0) > 0) {
            setLiveMessage("Canlı işlem tamamlandı. Bazı firmalarda hata oluştu.", "error");
          } else {
            setLiveMessage("Canlı işlem tamamlandı. Seçili firmalar için kullanıcılar oluşturuldu.", "success");
          }
        } catch (err) {
          setLiveMessage(err?.message || "Kullanıcı oluşturma başlatılamadı.", "error");
        } finally {
          form.dataset.liveSubmitting = "0";
          submitBtn.disabled = false;
          submitBtn.textContent = defaultLabel;
          form.classList.remove("is-loading");
          if (loadingMessage) loadingMessage.hidden = true;
        }
      });
    }

    applyInitialCompanySelection();
    if (isBulkMode) {
      renderBulkTemplateList();
      bulkTemplateModal?.addEventListener("keydown", (event) => {
        if (event.key === "Escape" && bulkTemplateModal?.classList.contains("active")) {
          closeBulkTemplateModal();
        }
      });
    }
  };

  const initObusUserDeactivateForm = () => {
    const searchForm = document.querySelector(".obus-user-deactivate-search-form");
    if (searchForm && searchForm.dataset.deactivateSearchBound !== "1") {
      searchForm.dataset.deactivateSearchBound = "1";
      const submitBtn = searchForm.querySelector("button[type='submit']");
      const loadingMessage = searchForm.querySelector(".obus-user-deactivate-search-loading-message");
      if (submitBtn) {
        const defaultLabel = String(submitBtn.textContent || "").trim() || "Kullanıcıları Listele";
        searchForm.classList.remove("is-loading");
        submitBtn.disabled = false;
        submitBtn.textContent = defaultLabel;
        if (loadingMessage) loadingMessage.hidden = true;
        searchForm.addEventListener("submit", () => {
          submitBtn.disabled = true;
          submitBtn.textContent = "Listeleniyor...";
          searchForm.classList.add("is-loading");
          if (loadingMessage) loadingMessage.hidden = false;
        });
      }
    }

    const deleteForm = document.querySelector(".obus-user-deactivate-delete-form");
    if (!deleteForm || deleteForm.dataset.deactivateDeleteBound === "1") return;
    deleteForm.dataset.deactivateDeleteBound = "1";

    const submitBtn = deleteForm.querySelector("button[type='submit']");
    const loadingMessage = deleteForm.querySelector(".obus-user-deactivate-delete-loading-message");
    const selectAllCheckbox = deleteForm.querySelector("[data-obus-user-select-all='1']");
    const itemCheckboxes = Array.from(deleteForm.querySelectorAll("[data-obus-user-item='1']"));
    const liveMessage = deleteForm.querySelector("[data-obus-user-deactivate-live-message='1']");
    const liveProgress = deleteForm.querySelector("[data-obus-live-progress='1']");
    const liveSuccess = deleteForm.querySelector("[data-obus-live-success='1']");
    const liveFailure = deleteForm.querySelector("[data-obus-live-failure='1']");
    const rowByKey = new Map();
    const wait = (ms) =>
      new Promise((resolve) => {
        window.setTimeout(resolve, Math.max(0, Number(ms) || 0));
      });
    const setLiveMessage = (text = "", kind = "") => {
      if (!liveMessage) return;
      liveMessage.textContent = String(text || "").trim();
      liveMessage.hidden = !liveMessage.textContent;
      liveMessage.className = `obus-live-message ${kind === "success" ? "inline-success" : "inline-alert"}`.trim();
    };
    const setLiveCounters = ({ processed = 0, total = 0, success = 0, failure = 0 } = {}) => {
      if (liveProgress) {
        liveProgress.textContent = `İşlenen: ${processed}/${total}`;
      }
      if (liveSuccess) {
        liveSuccess.textContent = `Başarılı: ${success}`;
      }
      if (liveFailure) {
        liveFailure.textContent = `Hatalı: ${failure}`;
      }
    };
    const shortenStatusText = (text, maxLength = 96) => {
      const normalized = String(text || "").replace(/\s+/g, " ").trim();
      if (!normalized) return "";
      if (normalized.length <= maxLength) return normalized;
      return `${normalized.slice(0, Math.max(0, maxLength - 3))}...`;
    };
    const setDeactivateRowStatus = (row, text, kind, detail = "") => {
      const cell = row?.querySelector("[data-obus-user-status-cell='1']");
      if (!cell) return;
      cell.textContent = String(text || "").trim() || "-";
      cell.className = `obus-live-status ${kind || "pending"}`;
      const tooltip = String(detail || "").trim();
      if (tooltip) {
        cell.setAttribute("title", tooltip);
        cell.classList.add("has-detail");
      } else {
        cell.removeAttribute("title");
        cell.classList.remove("has-detail");
      }
    };
    const applyDeactivateEventToRow = (eventItem) => {
      const key = String(eventItem?.key || "").trim();
      const row = rowByKey.get(key);
      if (!row) return;
      if (eventItem?.ok === true) {
        setDeactivateRowStatus(row, eventItem.message || "Pasife alındı", "success");
      } else {
        const errorText = String(eventItem?.error || "").trim() || "İşlem başarısız";
        const errorDetail = String(eventItem?.errorDetail || "").trim();
        const visibleText = shortenStatusText(errorDetail || errorText);
        const tooltipText = errorDetail && errorDetail !== errorText ? `${errorText} | ${errorDetail}` : errorDetail || errorText;
        setDeactivateRowStatus(row, visibleText, "failure", tooltipText);
      }
    };
    const pollLiveJob = async (jobId, onEvent) => {
      let cursor = 0;
      while (true) {
        if (!document.body.contains(deleteForm)) {
          throw new Error("Sayfa değiştiği için canlı takip durduruldu.");
        }
        const response = await fetch(`/api/obus-live/${encodeURIComponent(jobId)}?cursor=${cursor}`, {
          headers: { Accept: "application/json" }
        });
        const data = await parseJsonResponse(response);
        if (!response.ok || !data?.ok) {
          throw new Error(getApiErrorMessage(response, data, "Canlı işlem durumu okunamadı"));
        }

        const events = Array.isArray(data.events) ? data.events : [];
        events.forEach((eventItem) => {
          if (typeof onEvent === "function") onEvent(eventItem);
        });
        cursor = Number.isFinite(Number(data.cursor)) ? Number(data.cursor) : cursor;
        setLiveCounters({
          processed: Number(data.processedCount || 0),
          total: Number(data.totalCount || 0),
          success: Number(data.successCount || 0),
          failure: Number(data.failureCount || 0)
        });

        if (data.done) return data;
        await wait(450);
      }
    };

    const syncSelectAll = () => {
      if (!selectAllCheckbox) return;
      const enabledCheckboxes = itemCheckboxes.filter((item) => !item.disabled);
      if (enabledCheckboxes.length === 0) {
        selectAllCheckbox.checked = false;
        selectAllCheckbox.indeterminate = false;
        return;
      }
      const checkedCount = enabledCheckboxes.filter((item) => item.checked).length;
      selectAllCheckbox.checked = checkedCount === enabledCheckboxes.length;
      selectAllCheckbox.indeterminate = checkedCount > 0 && checkedCount < enabledCheckboxes.length;
    };

    const loadRows = () => {
      rowByKey.clear();
      const rows = Array.from(deleteForm.querySelectorAll("tr[data-obus-user-row-key]"));
      rows.forEach((row) => {
        const key = String(row.getAttribute("data-obus-user-row-key") || "").trim();
        if (!key) return;
        rowByKey.set(key, row);
      });
    };

    if (selectAllCheckbox) {
      selectAllCheckbox.addEventListener("change", () => {
        itemCheckboxes.forEach((item) => {
          if (item.disabled) return;
          item.checked = selectAllCheckbox.checked;
        });
        syncSelectAll();
      });
    }

    itemCheckboxes.forEach((item) => {
      item.addEventListener("change", () => {
        syncSelectAll();
      });
    });
    loadRows();
    syncSelectAll();

    if (submitBtn) {
      const defaultLabel = String(submitBtn.textContent || "").trim() || "Seçili Kullanıcıları Pasife Al";
      deleteForm.classList.remove("is-loading");
      submitBtn.disabled = false;
      submitBtn.textContent = defaultLabel;
      if (loadingMessage) loadingMessage.hidden = true;

      deleteForm.addEventListener("submit", async (event) => {
        event.preventDefault();
        if (deleteForm.dataset.liveSubmitting === "1") return;
        deleteForm.dataset.liveSubmitting = "1";
        setLiveMessage("", "");
        loadRows();

        const selectedCheckboxes = itemCheckboxes.filter((item) => item.checked && !item.disabled);
        if (selectedCheckboxes.length === 0) {
          setLiveMessage("En az bir kullanıcı seçmelisiniz.", "error");
          deleteForm.dataset.liveSubmitting = "0";
          return;
        }

        const selectedItems = selectedCheckboxes
          .map((checkbox) => {
            const key = String(checkbox.value || "").trim();
            const row = rowByKey.get(key);
            if (!row) return null;
            return {
              value: key,
              source: String(row.getAttribute("data-obus-user-row-source") || "").trim(),
              id: String(row.getAttribute("data-obus-user-row-id") || "").trim(),
              "partner-id": String(row.getAttribute("data-obus-user-row-partner-id") || "").trim(),
              username: String(row.getAttribute("data-obus-user-row-username") || "").trim(),
              code: String(row.getAttribute("data-obus-user-row-code") || "").trim()
            };
          })
          .filter(Boolean);

        if (selectedItems.length === 0) {
          setLiveMessage("Seçili kullanıcı satırları okunamadı.", "error");
          deleteForm.dataset.liveSubmitting = "0";
          return;
        }

        selectedItems.forEach((item) => {
          const row = rowByKey.get(String(item.value || "").trim());
          if (row) setDeactivateRowStatus(row, "İşleniyor...", "progress");
        });

        submitBtn.disabled = true;
        submitBtn.textContent = "Pasife Alınıyor...";
        deleteForm.classList.add("is-loading");
        if (loadingMessage) loadingMessage.hidden = false;
        const prevDisabled = itemCheckboxes.map((checkbox) => checkbox.disabled);
        itemCheckboxes.forEach((checkbox) => {
          checkbox.disabled = true;
        });
        if (selectAllCheckbox) {
          selectAllCheckbox.disabled = true;
        }

        try {
          const startResponse = await fetch("/api/obus-user-deactivate/live/start", {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
              Accept: "application/json"
            },
            body: JSON.stringify({
              selectedItems
            })
          });
          const startData = await parseJsonResponse(startResponse);
          if (!startResponse.ok || !startData?.ok) {
            throw new Error(getApiErrorMessage(startResponse, startData, "Pasife alma işlemi başlatılamadı"));
          }

          setLiveCounters({
            processed: 0,
            total: Number(startData.totalCount || selectedItems.length || 0),
            success: 0,
            failure: 0
          });

          const finalState = await pollLiveJob(startData.jobId, applyDeactivateEventToRow);
          if (finalState?.error) {
            setLiveMessage(finalState.error, "error");
          } else if (Number(finalState?.failureCount || 0) > 0) {
            setLiveMessage("Canlı pasife alma tamamlandı. Bazı kayıtlarda hata oluştu.", "error");
          } else {
            setLiveMessage("Canlı pasife alma tamamlandı. Seçili kullanıcılar pasife alındı.", "success");
          }
        } catch (err) {
          setLiveMessage(err?.message || "Pasife alma işlemi başlatılamadı.", "error");
        } finally {
          deleteForm.dataset.liveSubmitting = "0";
          submitBtn.disabled = false;
          submitBtn.textContent = defaultLabel;
          deleteForm.classList.remove("is-loading");
          if (loadingMessage) loadingMessage.hidden = true;
          itemCheckboxes.forEach((checkbox, index) => {
            checkbox.disabled = Boolean(prevDisabled[index]);
          });
          if (selectAllCheckbox) {
            selectAllCheckbox.disabled = false;
          }
          syncSelectAll();
        }
      });
    }
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
          });
      });
    });

    const itemCheckboxes = Array.from(form.querySelectorAll("[data-item-checkbox='1']"));
    itemCheckboxes.forEach((checkbox) => {
      checkbox.addEventListener("change", () => {
        if (!checkbox.checked) return;
        const sectionKey = String(checkbox.getAttribute("data-parent-section") || "").trim();
        if (!sectionKey) return;
        const sectionToggle = form.querySelector(`[data-section-toggle="${sectionKey}"]`);
        if (!sectionToggle || sectionToggle.disabled) return;
        sectionToggle.checked = true;
      });
    });
  };

  const initEndpointUI = async () => {
    initSalesTabs();
    initSalesReportLoading();
    initSlackReportLoading();
    initAllowedLinesLoading();
    initJourneySearchForm();
    initObusUserCreateBuilder();
    initObusUserDeactivateForm();
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
