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

  const parseJsonResponse = async (response) => {
    const contentType = response.headers.get("content-type") || "";
    if (!contentType.includes("application/json")) return null;
    try {
      return await response.json();
    } catch (err) {
      return null;
    }
  };

  const getApiErrorMessage = (response, data, fallback) => {
    if (data?.error) return data.error;
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

  const defaultBody = `{\n  \"type\": 1,\n  \"connection\": {\n    \"ip-address\": \"212.156.219.182\",\n    \"port\": \"5117\"\n  },\n  \"browser\": {\n    \"name\": \"Chrome\"\n  }\n}`;
  const defaultHeaders = "{\n  \"Content-Type\": \"application/json\"\n}";
  const defaultParams = "{}";

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
                ? `<button type="button" class="ghost small endpoint-edit" data-endpoint-id="${itemId}">Düzenle</button>`
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
    const targetInput = document.querySelector("#target-url-input");
    if (!title || !path || !method || !body || !headers || !params) return;
    title.textContent = item.title;
    path.textContent = `${item.method} ${item.path}`;
    method.textContent = item.method;
    body.value = item.body || defaultBody;
    headers.value = item.headers || defaultHeaders;
    params.value = item.params || defaultParams;
    if (editors?.headers) editors.headers.render();
    if (editors?.params) editors.params.render();
    if (targetInput) {
      targetInput.value = (item.targetUrl || "").trim();
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
    const targetInput = document.querySelector("#target-url-input");
    const targetList = document.querySelector("#target-url-list");
    if (!targetInput || !targetList) return;
    const currentValue = targetInput.value?.trim() || "";
    const targets = Array.from(
      new Set(
        endpoints
          .map((item) => item.targetUrl?.trim())
          .filter((value) => value)
      )
    );
    targetList.innerHTML = "";
    targets.forEach((url) => {
      const option = document.createElement("option");
      option.value = url;
      targetList.appendChild(option);
    });
    if (currentValue && targets.includes(currentValue)) {
      targetInput.value = currentValue;
    }
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

  const initEndpointUI = async () => {
    const modal = document.querySelector("#endpoint-modal");
    if (!modal) return;
    const openBtn = document.querySelector("#open-endpoint-modal");
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
    let selected = Number.isInteger(Number(endpoints[0]?.id)) ? Number(endpoints[0].id) : null;
    let editingEndpointId = null;

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
      const targetField = form.elements.namedItem("targetUrl");
      const methodField = form.elements.namedItem("method");
      const pathField = form.elements.namedItem("path");
      const descriptionField = form.elements.namedItem("description");

      if (!isEdit) {
        form.reset();
        if (targetField && targetInput?.value) {
          targetField.value = targetInput.value;
        }
        return;
      }

      if (titleField) titleField.value = item.title || "";
      if (targetField) targetField.value = item.targetUrl || "";
      if (methodField) methodField.value = (item.method || "GET").toUpperCase();
      if (pathField) pathField.value = item.path || "/";
      if (descriptionField) descriptionField.value = item.description || "";
    };

    const renderSelected = async (endpointId) => {
      if (!Number.isInteger(Number(endpointId))) return;
      selected = Number(endpointId);
      renderTable(endpoints, selected);
      renderTargets(endpoints);
      const current = endpoints.find((e) => Number(e.id) === selected);
      if (!current) return;
      renderDetails(current, { headers: headerEditor, params: paramEditor });
      if (hasHistoryPanel) {
        renderHistory(await loadRequests(selected));
        renderHistoryDetail(null);
      }
    };

    renderTable(endpoints, selected);
    renderTargets(endpoints);
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

    form?.addEventListener("submit", async (event) => {
      event.preventDefault();
      setModalFormStatus("", "");
      if (submitBtn) submitBtn.disabled = true;
      try {
        const data = new FormData(form);
        const normalized = normalizeEndpointAddress({
          targetUrl: data.get("targetUrl")?.toString() || "",
          path: data.get("path")?.toString() || ""
        });
        if (!normalized.targetUrl) {
          const msg = "Hedef URL zorunlu.";
          if (statusText) statusText.textContent = msg;
          setModalFormStatus(msg, "error");
          return;
        }

        const item = {
          title: data.get("title")?.toString().trim() || "Endpoint",
          method: data.get("method")?.toString().toUpperCase() || "GET",
          path: normalized.path,
          description: data.get("description")?.toString().trim() || "",
          targetUrl: normalized.targetUrl,
          body: defaultBody,
          headers: defaultHeaders,
          params: defaultParams
        };

        const isEdit = Number.isInteger(Number(editingEndpointId));
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
          renderTargets(endpoints);
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
        renderTargets(endpoints);
        const current = endpoints.find((e) => Number(e.id) === Number(selected));
        if (current) {
          renderDetails(current, { headers: headerEditor, params: paramEditor });
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
            params: current.params,
            targetUrl: current.targetUrl
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

    const saveTargetUrl = () => {
      const current = endpoints.find((e) => Number(e.id) === Number(selected));
      if (!current || !Number.isInteger(Number(current.id))) return;
      current.targetUrl = (targetInput?.value || "").trim();
      renderTargets(endpoints);
      updateEndpoint(current.id, {
        body: current.body,
        headers: current.headers,
        params: current.params,
        targetUrl: current.targetUrl
      });
    };
    let targetTimer;
    targetInput?.addEventListener("input", () => {
      clearTimeout(targetTimer);
      targetTimer = setTimeout(saveTargetUrl, 350);
    });
    targetInput?.addEventListener("change", saveTargetUrl);

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
      const current =
        endpoints.find((e) => Number(e.id) === Number(selected)) ||
        endpoints.find((e) => Number.isInteger(Number(e.id)));
      if (!current || !Number.isInteger(Number(current.id))) {
        throw new Error("Endpoint bulunamadı.");
      }
      const method = (current.method || "GET").toUpperCase();
      const normalized = normalizeEndpointAddress({
        targetUrl: targetInput?.value || current.targetUrl || "",
        path: current.path || "/"
      });
      if (!normalized.targetUrl) {
        throw new Error("Hedef URL seçilmeli.");
      }
      const headersText = document.querySelector("#endpoint-headers")?.value || "";
      const paramsText = document.querySelector("#endpoint-params")?.value || "";
      const bodyText = bodyTextarea?.value || "";
      const headers = parseJsonField(headersText, "Headers");
      const params = parseJsonField(paramsText, "Params");
      return {
        endpointId: Number(current.id),
        method,
        path: normalized.path,
        targetUrl: normalized.targetUrl,
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
          setResponseState({
            statusText: data?.error || `İstek başarısız (${response.status}).`,
            badgeText: "Hata",
            badgeClass: "muted",
            body: data?.details || data?.body || "{}"
          });
          return;
        }
        const formatted = prettifyResponse(data.body || "");
        const badgeClass = data.ok ? "success" : "muted";
        const allowHeader = data?.headers?.allow || data?.headers?.Allow || "";
        let statusLine = data.ok ? "Tamamlandı" : "Yanıt hata döndü";
        if (!data.ok && Number(data.status) === 405) {
          statusLine = allowHeader
            ? `405 Method Not Allowed (Allow: ${allowHeader})`
            : "405 Method Not Allowed (method/path kontrol et)";
        }
        setResponseState({
          statusText: statusLine,
          badgeText: `${data.status} ${data.statusText}`,
          badgeClass,
          body: formatted || "{}",
          url: data.url ? `URL: ${data.url}` : "",
          time: data.durationMs ? `Süre: ${data.durationMs} ms` : ""
        });
        if (hasHistoryPanel) {
          loadRequests(selected).then(renderHistory);
        }
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
