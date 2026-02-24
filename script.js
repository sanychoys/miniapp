(() => {
  const loader = document.getElementById("pageLoader");
  if (!loader) {
    return;
  }

  const minDurationMs = 1500;
  const startedAt = performance.now();
  let isWindowLoaded = document.readyState === "complete";

  const hideLoader = () => {
    if (!isWindowLoaded) {
      return;
    }

    const elapsed = performance.now() - startedAt;
    const remaining = Math.max(0, minDurationMs - elapsed);

    window.setTimeout(() => {
      loader.classList.add("is-hidden");
      loader.addEventListener(
        "transitionend",
        () => {
          loader.remove();
        },
        { once: true }
      );
    }, remaining);
  };

  if (!isWindowLoaded) {
    window.addEventListener(
      "load",
      () => {
        isWindowLoaded = true;
        hideLoader();
      },
      { once: true }
    );
  } else {
    hideLoader();
  }
})();

(() => {
  const panel = document.querySelector(".main-glass-panel");
  const tabs = document.querySelectorAll(".panel-tab");
  const views = document.querySelectorAll(".panel-view");
  if (!panel || !tabs.length || !views.length) {
    return;
  }

  const panelClasses = ["is-stars", "is-premium", "is-ton"];

  const activatePanel = (name) => {
    panel.classList.remove(...panelClasses);
    panel.classList.add(`is-${name}`);

    tabs.forEach((tab) => {
      const isActive = tab.dataset.panel === name;
      tab.classList.toggle("is-active", isActive);
      tab.setAttribute("aria-selected", isActive ? "true" : "false");
    });

    views.forEach((view) => {
      view.classList.toggle("is-active", view.dataset.view === name);
    });
  };

  tabs.forEach((tab) => {
    tab.addEventListener("click", () => {
      activatePanel(tab.dataset.panel);
    });
  });
})();

(() => {
  const grids = document.querySelectorAll(".offer-grid");
  if (!grids.length) {
    return;
  }

  grids.forEach((grid) => {
    const cards = Array.from(grid.querySelectorAll(".offer-card"));
    const panelView = grid.closest(".panel-view");
    const quantityInput = panelView?.querySelector('.form-input[type="number"]');

    const getCardAmount = (card) =>
      card.querySelector(".offer-card-amount")?.textContent?.trim() || "";

    const setActiveCard = (activeCard) => {
      cards.forEach((item) => item.classList.remove("is-active"));
      if (activeCard) {
        activeCard.classList.add("is-active");
      }
    };

    const findCardByAmount = (value) => {
      const normalized = String(value).trim();
      if (!normalized) {
        return null;
      }

      return (
        cards.find((card) => getCardAmount(card) === normalized) || null
      );
    };

    const applyCardToInput = (card) => {
      if (!quantityInput || !card) {
        return;
      }

      const amount = getCardAmount(card);
      if (!amount) {
        return;
      }

      quantityInput.value = amount;
      quantityInput.dispatchEvent(new Event("input", { bubbles: true }));
    };

    const initialActiveCard = grid.querySelector(".offer-card.is-active");
    if (initialActiveCard) {
      setActiveCard(initialActiveCard);
      applyCardToInput(initialActiveCard);
    }

    cards.forEach((card) => {
      card.addEventListener("click", () => {
        const isAlreadyActive = card.classList.contains("is-active");
        if (isAlreadyActive) {
          setActiveCard(null);
          return;
        }

        setActiveCard(card);
        applyCardToInput(card);
      });
    });

    if (quantityInput) {
      quantityInput.addEventListener("input", () => {
        const matchedCard = findCardByAmount(quantityInput.value);
        setActiveCard(matchedCard);
      });
    }
  });
})();

(() => {
  const usernameInputs = document.querySelectorAll(".form-input--telegram");
  if (!usernameInputs.length) {
    return;
  }

  const sanitizeTelegramUsername = (rawValue) => {
    const cleaned = rawValue.replace(/[^@A-Za-z0-9_]/g, "");
    const usernamePart = cleaned.replace(/@/g, "").slice(0, 32);

    if (!cleaned) {
      return "";
    }

    return `@${usernamePart}`;
  };

  usernameInputs.forEach((input) => {
    input.addEventListener("input", () => {
      const sanitizedValue = sanitizeTelegramUsername(input.value);
      if (input.value !== sanitizedValue) {
        input.value = sanitizedValue;
      }
    });
  });
})();
