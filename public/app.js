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

  const loadEndpoints = () => {
    try {
      return JSON.parse(localStorage.getItem("endpoints") || "[]");
    } catch {
      return [];
    }
  };

  const saveEndpoints = (items) => {
    localStorage.setItem("endpoints", JSON.stringify(items));
  };

  const defaultBody = `{\n  \"type\": 1,\n  \"connection\": {\n    \"ip-address\": \"212.156.219.182\",\n    \"port\": \"5117\"\n  },\n  \"browser\": {\n    \"name\": \"Chrome\"\n  }\n}`;

  const normalizeEndpoints = (items) =>
    items.map((item) => ({
      body: item.body || "{}",
      headers: item.headers || "{\n  \"Content-Type\": \"application/json\"\n}",
      params: item.params || "{}",
      targetUrl: item.targetUrl || "",
      ...item
    }))
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

  const seedIfEmpty = () => {
    const existing = loadEndpoints();
    if (existing.length) {
      const normalized = normalizeEndpoints(existing);
      saveEndpoints(normalized);
      return normalized;
    }
    const seeded = [
      {
        id: "getsession",
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
    saveEndpoints(seeded);
    return seeded;
  };

  const renderSidebar = (items, selectedId) => {
    const list = document.querySelector("#endpoint-list");
    if (!list) return;
    list.innerHTML = "";
    items.forEach((item) => {
      const button = document.createElement("button");
      const isActive = item.id === selectedId;
      button.className = `api-item${isActive ? " active" : ""}`;
      button.dataset.endpointId = item.id;
      button.innerHTML = `<span class="method ${item.method.toLowerCase()}">${item.method}</span><span>${item.path}</span>`;
      list.appendChild(button);
    });
  };

  const renderTable = (items) => {
    const table = document.querySelector("#endpoint-table");
    if (!table) return;
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
  };

  const renderDetails = (item) => {
    const title = document.querySelector("#endpoint-title");
    const path = document.querySelector("#endpoint-path");
    const method = document.querySelector("#endpoint-method");
    const body = document.querySelector("#endpoint-body");
    const headers = document.querySelector("#endpoint-headers");
    const params = document.querySelector("#endpoint-params");
    if (!title || !path || !method || !body || !headers || !params) return;
    title.textContent = item.title;
    path.textContent = `${item.method} ${item.path}`;
    method.textContent = item.method;
    body.value = item.body || "";
    headers.value = item.headers || "";
    params.value = item.params || "";
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

  const initEndpointUI = () => {
    const modal = document.querySelector("#endpoint-modal");
    if (!modal) return;
    const openBtn = document.querySelector("#open-endpoint-modal");
    const closeBtn = document.querySelector("#close-endpoint-modal");
    const form = document.querySelector("#endpoint-form");
    const list = document.querySelector("#endpoint-list");
    const targetSelect = document.querySelector(".target-select");

    let endpoints = seedIfEmpty();
    let selected = endpoints[0]?.id;

    renderSidebar(endpoints, selected);
    renderTable(endpoints);
    if (endpoints[0]) renderDetails(endpoints[0]);

    const openModal = () => {
      modal.classList.add("active");
      modal.setAttribute("aria-hidden", "false");
    };

    const closeModal = () => {
      modal.classList.remove("active");
      modal.setAttribute("aria-hidden", "true");
    };

    openBtn?.addEventListener("click", openModal);
    closeBtn?.addEventListener("click", closeModal);
    modal.addEventListener("click", (event) => {
      if (event.target === modal) closeModal();
    });

    list?.addEventListener("click", (event) => {
      const item = event.target.closest(".api-item");
      if (!item) return;
      selected = item.dataset.endpointId;
      renderSidebar(endpoints, selected);
      const current = endpoints.find((e) => e.id === selected);
      if (current) renderDetails(current);
    });

    form?.addEventListener("submit", (event) => {
      event.preventDefault();
      const data = new FormData(form);
      const id = `${Date.now()}`;
      const item = {
        id,
        title: data.get("title")?.toString().trim() || "Endpoint",
        method: data.get("method")?.toString().toUpperCase() || "GET",
        path: data.get("path")?.toString().trim() || "/",
        description: data.get("description")?.toString().trim() || "",
        targetUrl: data.get("targetUrl")?.toString().trim() || "",
        body: defaultBody,
        headers: "{\n  \"Content-Type\": \"application/json\"\n}",
        params: "{}"
      };
      endpoints = [item, ...endpoints];
      saveEndpoints(endpoints);
      renderSidebar(endpoints, item.id);
      renderTable(endpoints);
      renderDetails(item);
      if (targetSelect) {
        const urls = Array.from(
          new Set(
            endpoints
              .map((endpoint) => endpoint.targetUrl?.trim())
              .filter((value) => value)
          )
        );
        targetSelect.innerHTML = "";
        if (!urls.length) {
          const option = document.createElement("option");
          option.textContent = "Seçiniz";
          option.value = "";
          targetSelect.appendChild(option);
        } else {
          urls.forEach((url) => {
            const option = document.createElement("option");
            option.textContent = url;
            option.value = url;
            targetSelect.appendChild(option);
          });
        }
      }
      form.reset();
      closeModal();
    });

    const bindSave = (field, key) => {
      field?.addEventListener("input", () => {
        const current = endpoints.find((e) => e.id === selected);
        if (!current) return;
        current[key] = field.value;
        saveEndpoints(endpoints);
      });
    };

    bindSave(document.querySelector("#endpoint-body"), "body");
    bindSave(document.querySelector("#endpoint-headers"), "headers");
    bindSave(document.querySelector("#endpoint-params"), "params");

    initTabs();

    if (targetSelect) {
      const targets = Array.from(
        new Set(
          endpoints
            .map((item) => item.targetUrl?.trim())
            .filter((value) => value)
        )
      );
      targetSelect.innerHTML = "";
      if (!targets.length) {
        const option = document.createElement("option");
        option.textContent = "Seçiniz";
        option.value = "";
        targetSelect.appendChild(option);
      } else {
        targets.forEach((url) => {
          const option = document.createElement("option");
          option.textContent = url;
          option.value = url;
          targetSelect.appendChild(option);
        });
      }
    }
  };

  initEndpointUI();
})();
