(() => {
  const sidebar = document.querySelector(".sidebar");
  const content = document.querySelector(".content");

  if (!sidebar || !content) return;

  const isModified = (event) =>
    event.metaKey || event.ctrlKey || event.shiftKey || event.altKey;

  const routeFromPath = (path) => {
    if (path.startsWith("/users")) return "users";
    if (path.startsWith("/screens")) return "screens";
    if (path.startsWith("/change-password")) return "password";
    return "dashboard";
  };

  const setActive = (path) => {
    const route = routeFromPath(path);
    document.querySelectorAll(".nav-item").forEach((item) => {
      item.classList.toggle("active", item.dataset.route === route);
    });
  };

  const replaceContent = (html, url, push = true) => {
    const doc = new DOMParser().parseFromString(html, "text/html");
    const next = doc.querySelector(".content");
    if (!next) return false;

    content.innerHTML = next.innerHTML;
    document.title = doc.title || document.title;
    setActive(new URL(url).pathname);
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
    const link = event.target.closest("a.nav-item");
    if (!link) return;
    if (link.target || isModified(event)) return;
    event.preventDefault();
    navigate(link.href);
  });

  window.addEventListener("popstate", () => {
    navigate(window.location.href, { push: false });
  });

  const loadEndpoints = async () => {
    const response = await fetch("/api/endpoints");
    if (!response.ok) return [];
    const data = await response.json();
    return data.items || [];
  };

  const saveEndpoint = async (payload) => {
    const response = await fetch("/api/endpoints", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload)
    });
    if (!response.ok) return null;
    const data = await response.json();
    return data.item || null;
  };

  const loadRequests = async (endpointId) => {
    if (!endpointId) return [];
    const response = await fetch(`/api/requests/${endpointId}`);
    if (!response.ok) return [];
    const data = await response.json();
    return data.items || [];
  };

  const loadRequestDetail = async (id) => {
    if (!id) return null;
    const response = await fetch(`/api/requests/item/${id}`);
    if (!response.ok) return null;
    const data = await response.json();
    return data.item || null;
  };

  const clearRequests = async (endpointId) => {
    if (!endpointId) return false;
    const response = await fetch(`/api/requests/${endpointId}`, { method: "DELETE" });
    return response.ok;
  };

  const updateEndpoint = async (id, payload) => {
    await fetch(`/api/endpoints/${id}`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload)
    });
  };

  const defaultBody = `{\n  \"type\": 1,\n  \"connection\": {\n    \"ip-address\": \"212.156.219.182\",\n    \"port\": \"5117\"\n  },\n  \"browser\": {\n    \"name\": \"Chrome\"\n  }\n}`;

  const normalizeEndpoints = (items) =>
    items.map((item) => {
      const targetUrl = item.targetUrl || item.target_url || "";
      return {
        body: item.body || "{}",
        headers: item.headers || "{\n  \"Content-Type\": \"application/json\"\n}",
        params: item.params || "{}",
        targetUrl,
        ...item
      };
    })
    .map((item) => {
      const trimmedPath = item.path?.trim() || "/";
      if (!item.targetUrl && /^https?:\/\//i.test(trimmedPath)) {
        return {
          ...item,
          targetUrl: trimmedPath,
          path: "/"
        };
      }
      return item;
    }));

  const seedIfEmpty = async () => {
    const existing = await loadEndpoints();
    if (existing.length) {
      return normalizeEndpoints(existing);
    }
    const seeded = [
      {
        id: null,
        title: "GetSession",
        method: "POST",
        path: "/GetSession",
        description: "Session başlatma",
        targetUrl: "",
        body: defaultBody,
        headers: "{\n  \"Content-Type\": \"application/json\"\n}",
        params: "{}"
      }
    ];
    const created = await saveEndpoint(seeded[0]);
    return created ? normalizeEndpoints([created]) : normalizeEndpoints(seeded);
  };

  const renderSidebar = (items, selectedId) => {
    const list = document.querySelector("#endpoint-list");
    if (!list) return;
    list.innerHTML = "";
    items.forEach((item) => {
      const button = document.createElement("button");
      const isActive = Number(item.id) === Number(selectedId);
      button.className = `api-item${isActive ? " active" : ""}`;
      button.dataset.endpointId = item.id;
      button.innerHTML = `<span class="method ${item.method.toLowerCase()}">${item.method}</span><span>${item.path}</span>`;
      list.appendChild(button);
    });
  };

  const renderTable = (items) => {
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
        row.className = "endpoint-row";
        row.innerHTML = `
          <span class="method ${item.method.toLowerCase()}">${item.method}</span>
          <div>
            <div class="path">${item.path}</div>
            <div class="desc">${item.title}${item.description ? " — " + item.description : ""}</div>
          </div>
          <span></span>
        `;
        table.appendChild(row);
      });
    });
  };

  const renderDetails = (item, editors) => {
    const title = document.querySelector("#endpoint-title");
    const path = document.querySelector("#endpoint-path");
    const method = document.querySelector("#endpoint-method");
    const headers = document.querySelector("#endpoint-headers");
    const params = document.querySelector("#endpoint-params");
    const targetSelect = document.querySelector("#target-url-select");
    if (!title || !path || !method || !headers || !params) return;
    title.textContent = item.title;
    path.textContent = `${item.method} ${item.path}`;
    method.textContent = item.method;
    headers.value = item.headers || "{}";
    params.value = item.params || "{}";
    if (editors?.headers) editors.headers.render();
    if (editors?.params) editors.params.render();
    if (targetSelect) {
      targetSelect.value = item.targetUrl || "";
    }
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

  const renderTargets = (endpoints) => {
    const targetSelect = document.querySelector("#target-url-select");
    if (!targetSelect) return;
    const targets = Array.from(
      new Set(
        endpoints
          .map((item) => item.targetUrl?.trim())
          .filter((value) => value)
      )
    );
    targetSelect.innerHTML = "";
    const placeholder = document.createElement("option");
    placeholder.textContent = "Seçiniz";
    placeholder.value = "";
    targetSelect.appendChild(placeholder);
    targets.forEach((url) => {
      const option = document.createElement("option");
      option.textContent = url;
      option.value = url;
      targetSelect.appendChild(option);
    });
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

  const initEndpointUI = async () => {
    const modal = document.querySelector("#endpoint-modal");
    if (!modal) return;
    const openBtn = document.querySelector("#open-endpoint-modal");
    const closeBtn = document.querySelector("#close-endpoint-modal");
    const form = document.querySelector("#endpoint-form");
    const list = document.querySelector("#endpoint-list");
    const sendBtn = document.querySelector("#send-request");
    const statusText = document.querySelector("#request-status");
    const responseStatus = document.querySelector("#response-status");
    const responseBody = document.querySelector("#response-body");
    const responseUrl = document.querySelector("#response-url");
    const responseTime = document.querySelector("#response-time");
    const targetSelect = document.querySelector("#target-url-select");
    const historyList = document.querySelector("#request-history");
    const clearHistoryBtn = document.querySelector("#clear-history");
    const headersRows = document.querySelector("#headers-rows");
    const paramsRows = document.querySelector("#params-rows");
    const addHeaderRow = document.querySelector("#add-header-row");
    const addParamRow = document.querySelector("#add-param-row");
    const headersTextarea = document.querySelector("#endpoint-headers");
    const paramsTextarea = document.querySelector("#endpoint-params");
    const copyHeadersJson = document.querySelector("#copy-headers-json");
    const copyParamsJson = document.querySelector("#copy-params-json");
    const copyBodyBtn = document.querySelector("#copy-body");
    const fixedBodyContent = document.querySelector("#fixed-body-content");

    let endpoints = await seedIfEmpty();
    let selected = endpoints[0]?.id ?? null;

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

    renderSidebar(endpoints, selected);
    renderTable(endpoints);
    renderTargets(endpoints);
    if (endpoints[0]) renderDetails(endpoints[0], { headers: headerEditor, params: paramEditor });
    renderHistory(await loadRequests(selected));
    renderHistoryDetail(null);

    const openModal = () => {
      modal.classList.add("active");
      modal.setAttribute("aria-hidden", "false");
    };

    const closeModal = () => {
      modal.classList.remove("active");
      modal.setAttribute("aria-hidden", "true");
    };

    if (!window.__endpointModalBound) {
      window.__endpointModalBound = true;
      document.addEventListener("click", (event) => {
        const openTarget = event.target.closest("#open-endpoint-modal");
        const closeTarget = event.target.closest("#close-endpoint-modal");
        const currentModal = document.querySelector("#endpoint-modal");
        if (!currentModal) return;
        if (openTarget) {
          currentModal.classList.add("active");
          currentModal.setAttribute("aria-hidden", "false");
        }
        if (closeTarget || event.target === currentModal) {
          currentModal.classList.remove("active");
          currentModal.setAttribute("aria-hidden", "true");
        }
      });
    }

    openBtn?.addEventListener("click", openModal);
    closeBtn?.addEventListener("click", closeModal);

    list?.addEventListener("click", (event) => {
      const item = event.target.closest(".api-item");
      if (!item) return;
      selected = Number(item.dataset.endpointId);
      renderSidebar(endpoints, selected);
      const current = endpoints.find((e) => e.id === selected);
      if (current) renderDetails(current, { headers: headerEditor, params: paramEditor });
      loadRequests(selected).then(renderHistory);
      renderHistoryDetail(null);
    });

    form?.addEventListener("submit", async (event) => {
      event.preventDefault();
      const data = new FormData(form);
      const item = {
        title: data.get("title")?.toString().trim() || "Endpoint",
        method: data.get("method")?.toString().toUpperCase() || "GET",
        path: data.get("path")?.toString().trim() || "/",
        description: data.get("description")?.toString().trim() || "",
        targetUrl: data.get("targetUrl")?.toString().trim() || "",
        body: defaultBody,
        headers: "{\n  \"Content-Type\": \"application/json\"\n}",
        params: "{}"
      };
      const created = await saveEndpoint(item);
      if (!created) return;
      endpoints = normalizeEndpoints(await loadEndpoints());
      const nextSelected = endpoints[0]?.id || created.id;
      renderSidebar(endpoints, nextSelected);
      renderTable(endpoints);
      renderTargets(endpoints);
      const current = endpoints.find((e) => e.id === nextSelected) || created;
      renderDetails(current, { headers: headerEditor, params: paramEditor });
      loadRequests(nextSelected).then(renderHistory);
      renderHistoryDetail(null);
      form.reset();
      closeModal();
    });

    const bindSave = (field, key) => {
      let timer;
      field?.addEventListener("input", () => {
        const current = endpoints.find((e) => e.id === selected);
        if (!current) return;
        current[key] = field.value;
        clearTimeout(timer);
        timer = setTimeout(() => {
          updateEndpoint(current.id, {
            body: current.body,
            headers: current.headers,
            params: current.params,
            targetUrl: current.targetUrl
          });
        }, 500);
      });
    };

    bindSave(document.querySelector("#endpoint-headers"), "headers");
    bindSave(document.querySelector("#endpoint-params"), "params");

    if (fixedBodyContent && !fixedBodyContent.textContent.trim()) {
      fixedBodyContent.textContent = defaultBody;
    }
    copyBodyBtn?.addEventListener("click", async () => {
      const text = fixedBodyContent?.textContent || defaultBody;
      if (navigator.clipboard?.writeText) {
        await navigator.clipboard.writeText(text);
      } else {
        window.prompt("Kopyala:", text);
      }
    });

    targetSelect?.addEventListener("change", () => {
      const current = endpoints.find((e) => e.id === selected);
      if (!current) return;
      current.targetUrl = targetSelect.value;
      updateEndpoint(current.id, {
        body: current.body,
        headers: current.headers,
        params: current.params,
        targetUrl: current.targetUrl
      });
    });

    const setResponseState = (state) => {
      if (statusText) statusText.textContent = state.statusText || "";
      if (responseStatus) {
        responseStatus.textContent = state.badgeText || "Bekleniyor";
        responseStatus.className = `pill ${state.badgeClass || "muted"}`.trim();
      }
      if (responseBody) responseBody.textContent = state.body || "{}";
      if (responseUrl) responseUrl.textContent = state.url || "";
      if (responseTime) responseTime.textContent = state.time || "";
    };

    const parseJsonField = (value, label) => {
      if (!value || !value.trim()) return {};
      try {
        return JSON.parse(value);
      } catch (err) {
        throw new Error(`${label} JSON hatalı.`);
      }
    };

    const buildPayload = () => {
      const current = endpoints.find((e) => e.id === selected) || endpoints[0];
      if (!current) throw new Error("Endpoint bulunamadı.");
      const method = (current.method || "GET").toUpperCase();
      const path = current.path || "/";
      const targetUrlValue = targetSelect?.value || current.targetUrl || "";
      if (!targetUrlValue && !/^https?:\/\//i.test(path)) {
        throw new Error("Hedef URL seçilmeli.");
      }
      const headersText = document.querySelector("#endpoint-headers")?.value || "";
      const paramsText = document.querySelector("#endpoint-params")?.value || "";
      const bodyText = defaultBody;
      const headers = parseJsonField(headersText, "Headers");
      const params = parseJsonField(paramsText, "Params");
      return {
        endpointId: current.id,
        method,
        path,
        targetUrl: targetUrlValue,
        headers,
        params,
        body: bodyText.trim() ? bodyText : ""
      };
    };

    const prettifyResponse = (payload) => {
      try {
        return JSON.stringify(JSON.parse(payload), null, 2);
      } catch (err) {
        return payload || "";
      }
    };

    sendBtn?.addEventListener("click", async () => {
      setResponseState({
        statusText: "İstek gönderiliyor...",
        badgeText: "İşleniyor",
        badgeClass: "muted",
        body: "..."
      });
      let payload;
      try {
        payload = buildPayload();
      } catch (err) {
        setResponseState({
          statusText: err.message,
          badgeText: "Hata",
          badgeClass: "muted",
          body: "{}"
        });
        return;
      }
      try {
        const response = await fetch("/api/execute", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload)
        });
        const data = await response.json();
        if (!data || data.error) {
          setResponseState({
            statusText: data?.error || "İstek başarısız.",
            badgeText: "Hata",
            badgeClass: "muted",
            body: data?.details || "{}"
          });
          return;
        }
        const formatted = prettifyResponse(data.body || "");
        const badgeClass = data.ok ? "success" : "muted";
        setResponseState({
          statusText: data.ok ? "Tamamlandı" : "Yanıt hata döndü",
          badgeText: `${data.status} ${data.statusText}`,
          badgeClass,
          body: formatted || "{}",
          url: data.url ? `URL: ${data.url}` : "",
          time: data.durationMs ? `Süre: ${data.durationMs} ms` : ""
        });
        loadRequests(selected).then(renderHistory);
      } catch (err) {
        setResponseState({
          statusText: "İstek hatası.",
          badgeText: "Hata",
          badgeClass: "muted",
          body: err.message || "{}"
        });
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
