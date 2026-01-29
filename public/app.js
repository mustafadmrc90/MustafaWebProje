(() => {
  const sidebar = document.querySelector(".sidebar");
  const content = document.querySelector(".content");
  const modal = document.querySelector("#endpoint-modal");

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

  if (modal) {
    const openBtn = document.querySelector("#open-endpoint-modal");
    const closeBtn = document.querySelector("#close-endpoint-modal");
    const form = document.querySelector("#endpoint-form");
    const list = document.querySelector("#endpoint-list");
    const table = document.querySelector("#endpoint-table");

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

    const renderSidebar = (items) => {
      if (!list) return;
      list.innerHTML = "";
      items.forEach((item, index) => {
        const button = document.createElement("button");
        button.className = `api-item${index === 0 ? " active" : ""}`;
        button.innerHTML = `<span class="method ${item.method.toLowerCase()}">${item.method}</span><span>${item.path}</span>`;
        list.appendChild(button);
      });
    };

    const renderTable = (items) => {
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

    const endpoints = loadEndpoints();
    renderSidebar(endpoints);
    renderTable(endpoints);

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

    form?.addEventListener("submit", (event) => {
      event.preventDefault();
      const data = new FormData(form);
      const item = {
        title: data.get("title")?.toString().trim() || "Endpoint",
        method: data.get("method")?.toString().toUpperCase() || "GET",
        path: data.get("path")?.toString().trim() || "/",
        description: data.get("description")?.toString().trim() || ""
      };
      const next = [item, ...loadEndpoints()];
      saveEndpoints(next);
      renderSidebar(next);
      renderTable(next);
      form.reset();
      closeModal();
    });
  }
})();
