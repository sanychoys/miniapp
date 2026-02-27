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
  const webApp = window.Telegram?.WebApp;
  if (!webApp) {
    return;
  }

  webApp.ready();
  webApp.expand();
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
  const dock = document.getElementById("appDock");
  const dockOrb = document.getElementById("appDockOrb");
  const dockButtons = Array.from(document.querySelectorAll(".app-dock-button"));
  const screens = Array.from(document.querySelectorAll(".app-screen"));
  if (!dock || !dockOrb || !dockButtons.length || !screens.length) {
    return;
  }

  let selectedScreen = "home";
  let activePointerId = null;
  let isDragging = false;
  let lastPointerX = 0;
  let pendingPointerX = null;
  let pointerFrameId = null;

  const clamp = (value, min, max) => Math.min(max, Math.max(min, value));

  const getDockBounds = () => dock.getBoundingClientRect();

  const buildLabelGlyphs = (labelNode) => {
    if (!(labelNode instanceof HTMLElement)) {
      return;
    }

    const hasGlyphMarkup = Boolean(labelNode.querySelector(".dock-glyph"));
    const rawText = hasGlyphMarkup
      ? labelNode.dataset.rawLabelText || labelNode.textContent || ""
      : labelNode.textContent || "";
    labelNode.dataset.rawLabelText = rawText;
    labelNode.textContent = "";

    Array.from(rawText).forEach((char) => {
      const glyph = document.createElement("span");
      glyph.className = "dock-glyph";
      glyph.style.setProperty("--glyph-scale-x", "1");
      glyph.style.setProperty("--glyph-skew-x", "0deg");
      if (char === " ") {
        glyph.classList.add("dock-glyph--space");
        glyph.textContent = "\u00A0";
      } else {
        glyph.textContent = char;
      }
      labelNode.appendChild(glyph);
    });
  };

  const syncDockLabelGlyphs = () => {
    dock.querySelectorAll(".app-dock-label").forEach((label) => {
      buildLabelGlyphs(label);
    });
  };

  const setGlyphDistortionForLabel = (labelNode, lensRect) => {
    if (!(labelNode instanceof HTMLElement)) {
      return;
    }

    const glyphNodes = labelNode.querySelectorAll(".dock-glyph");
    glyphNodes.forEach((glyphNode) => {
      const glyphRect = glyphNode.getBoundingClientRect();
      const glyphWidth = Math.max(glyphRect.width, 1);
      const overlapLeft = Math.max(glyphRect.left, lensRect.left);
      const overlapRight = Math.min(glyphRect.right, lensRect.right);
      const overlapWidth = Math.max(0, overlapRight - overlapLeft);

      if (overlapWidth <= 0) {
        glyphNode.style.setProperty("--glyph-scale-x", "1");
        glyphNode.style.setProperty("--glyph-skew-x", "0deg");
        return;
      }

      const glyphCenter = glyphRect.left + glyphRect.width / 2;
      const nearestEdgeDistance = Math.min(
        Math.abs(glyphCenter - lensRect.left),
        Math.abs(glyphCenter - lensRect.right)
      );

      const coverage = overlapWidth / glyphWidth;
      const edgeRange = Math.max(6, glyphWidth * 2.6);
      const edgeInfluence = clamp(1 - nearestEdgeDistance / edgeRange, 0, 1);
      const partialCoverage = Math.sin(Math.PI * clamp(coverage, 0, 1));
      const overlapCenter = overlapLeft + overlapWidth / 2;
      const centerOffset = clamp((overlapCenter - glyphCenter) / (glyphWidth / 2), -1, 1);
      const stretch = 1 + partialCoverage * edgeInfluence * 1.18;
      const skewX = centerOffset * partialCoverage * edgeInfluence * 20;

      glyphNode.style.setProperty("--glyph-scale-x", stretch.toFixed(3));
      glyphNode.style.setProperty("--glyph-skew-x", `${skewX.toFixed(3)}deg`);
    });
  };

  const resetGlyphDistortionForButton = (button) => {
    button.querySelectorAll(".dock-glyph").forEach((glyphNode) => {
      glyphNode.style.setProperty("--glyph-scale-x", "1");
      glyphNode.style.setProperty("--glyph-skew-x", "0deg");
    });
  };

  const getButtonCenterX = (button) => {
    const rect = button.getBoundingClientRect();
    return rect.left + rect.width / 2;
  };

  const setOrbPosition = (clientX) => {
    const bounds = getDockBounds();
    const orbRect = dockOrb.getBoundingClientRect();
    const halfOrbWidth = orbRect.width / 2;
    const minX = Math.min(halfOrbWidth, bounds.width / 2);
    const maxX = Math.max(bounds.width - halfOrbWidth, bounds.width / 2);
    const normalized = clamp(clientX - bounds.left, minX, maxX);
    dockOrb.style.setProperty("--orb-x", `${normalized}px`);
    lastPointerX = clientX;
  };

  const setLensClipForNode = (targetNode, lensRect) => {
    if (!(targetNode instanceof Element)) {
      return;
    }

    const targetRect = targetNode.getBoundingClientRect();
    const overlapLeft = Math.max(targetRect.left, lensRect.left);
    const overlapRight = Math.min(targetRect.right, lensRect.right);
    const overlapWidth = Math.max(0, overlapRight - overlapLeft);

    if (overlapWidth <= 0 || targetRect.width <= 0) {
      targetNode.style.setProperty("--lens-clip-start", "0%");
      targetNode.style.setProperty("--lens-clip-end", "0%");
      targetNode.style.setProperty("--lens-edge-left", "0");
      targetNode.style.setProperty("--lens-edge-right", "0");
      return;
    }

    const startPercent = ((overlapLeft - targetRect.left) / targetRect.width) * 100;
    const endPercent = ((overlapRight - targetRect.left) / targetRect.width) * 100;
    targetNode.style.setProperty("--lens-clip-start", `${startPercent.toFixed(3)}%`);
    targetNode.style.setProperty("--lens-clip-end", `${endPercent.toFixed(3)}%`);

    const edgeRange = Math.max(8, targetRect.width * 0.3);
    const leftInside = lensRect.left >= targetRect.left && lensRect.left <= targetRect.right;
    const rightInside = lensRect.right >= targetRect.left && lensRect.right <= targetRect.right;

    let leftPull = 0;
    let rightPull = 0;

    if (leftInside) {
      const leftDistance = Math.abs(lensRect.left - targetRect.left);
      leftPull = Math.max(0, Math.min(1, 1 - leftDistance / edgeRange));
    }

    if (rightInside) {
      const rightDistance = Math.abs(targetRect.right - lensRect.right);
      rightPull = Math.max(0, Math.min(1, 1 - rightDistance / edgeRange));
    }

    targetNode.style.setProperty("--lens-edge-left", leftPull.toFixed(3));
    targetNode.style.setProperty("--lens-edge-right", rightPull.toFixed(3));
  };

  const resetLensClip = (button) => {
    const iconWrap = button.querySelector(".app-dock-icon-wrap");
    const labelWrap = button.querySelector(".app-dock-label-wrap");
    iconWrap?.style.setProperty("--lens-clip-start", "0%");
    iconWrap?.style.setProperty("--lens-clip-end", "0%");
    iconWrap?.style.setProperty("--lens-edge-left", "0");
    iconWrap?.style.setProperty("--lens-edge-right", "0");
    labelWrap?.style.setProperty("--lens-clip-start", "0%");
    labelWrap?.style.setProperty("--lens-clip-end", "0%");
    labelWrap?.style.setProperty("--lens-edge-left", "0");
    labelWrap?.style.setProperty("--lens-edge-right", "0");
  };

  const setButtonHighlightByX = (clientX) => {
    const lensRect = dockOrb.getBoundingClientRect();

    dockButtons.forEach((button) => {
      const buttonRect = button.getBoundingClientRect();
      const centerX = getButtonCenterX(button);
      const distance = Math.abs(centerX - clientX);
      const radius = Math.max(buttonRect.width * 1.2, 84);
      const intensity = Math.max(0, Math.min(1, 1 - distance / radius));
      button.style.setProperty("--dock-lens", intensity.toFixed(3));

      setLensClipForNode(button.querySelector(".app-dock-icon-wrap"), lensRect);
      setLensClipForNode(button.querySelector(".app-dock-label-wrap"), lensRect);
      button.querySelectorAll(".app-dock-label").forEach((labelNode) => {
        setGlyphDistortionForLabel(labelNode, lensRect);
      });
    });
  };

  const applyPointerPosition = (clientX) => {
    setOrbPosition(clientX);
    setButtonHighlightByX(clientX);
  };

  const flushPointerFrame = () => {
    pointerFrameId = null;
    if (!isDragging || pendingPointerX === null) {
      return;
    }
    applyPointerPosition(pendingPointerX);
  };

  const queuePointerFrame = (clientX) => {
    pendingPointerX = clientX;
    if (pointerFrameId !== null) {
      return;
    }
    pointerFrameId = window.requestAnimationFrame(flushPointerFrame);
  };

  const setSelectedButton = (targetScreen) => {
    selectedScreen = targetScreen;
    dockButtons.forEach((button) => {
      const isSelected = button.dataset.target === targetScreen;
      button.classList.toggle("is-selected", isSelected);
      if (isSelected) {
        button.setAttribute("aria-current", "page");
      } else {
        button.removeAttribute("aria-current");
      }
      button.style.setProperty("--dock-lens", "0");
      resetLensClip(button);
      resetGlyphDistortionForButton(button);
    });
  };

  const scrollToPageStart = () => {
    window.scrollTo({ top: 0, left: 0, behavior: "auto" });
    if (document.documentElement) {
      document.documentElement.scrollTop = 0;
    }
    if (document.body) {
      document.body.scrollTop = 0;
    }
  };

  const setActiveScreen = (targetScreen) => {
    const knownScreen = screens.some((screen) => screen.dataset.screen === targetScreen);
    const nextScreen = knownScreen ? targetScreen : "home";

    screens.forEach((screen) => {
      const isActive = screen.dataset.screen === nextScreen;
      screen.classList.toggle("is-active", isActive);
      screen.setAttribute("aria-hidden", isActive ? "false" : "true");
    });

    setSelectedButton(nextScreen);
    scrollToPageStart();
  };

  const findClosestButton = (clientX) => {
    let closestButton = dockButtons[0];
    let smallestDistance = Number.POSITIVE_INFINITY;

    dockButtons.forEach((button) => {
      const distance = Math.abs(getButtonCenterX(button) - clientX);
      if (distance < smallestDistance) {
        smallestDistance = distance;
        closestButton = button;
      }
    });

    return closestButton;
  };

  const alignOrbToSelected = () => {
    const selectedButton =
      dockButtons.find((button) => button.dataset.target === selectedScreen) || dockButtons[0];
    setOrbPosition(getButtonCenterX(selectedButton));
  };

  const finishDragging = () => {
    if (!isDragging) {
      return;
    }

    isDragging = false;
    dock.classList.remove("is-dragging");

    const closestButton = findClosestButton(lastPointerX);
    setActiveScreen(closestButton.dataset.target || "home");
    alignOrbToSelected();

    dockButtons.forEach((button) => {
      button.style.setProperty("--dock-lens", "0");
      resetLensClip(button);
      resetGlyphDistortionForButton(button);
    });

    if (pointerFrameId !== null) {
      window.cancelAnimationFrame(pointerFrameId);
      pointerFrameId = null;
    }
    pendingPointerX = null;
    activePointerId = null;
  };

  dockButtons.forEach((button) => {
    button.addEventListener("click", (event) => {
      if (isDragging) {
        event.preventDefault();
        return;
      }
      setActiveScreen(button.dataset.target || "home");
      alignOrbToSelected();
    });
  });

  dock.addEventListener("pointerdown", (event) => {
    if (!(event.target instanceof Element)) {
      return;
    }
    if (!event.target.closest(".app-dock-button")) {
      return;
    }

    isDragging = true;
    activePointerId = event.pointerId;
    dock.classList.add("is-dragging");
    applyPointerPosition(event.clientX);
    dock.setPointerCapture?.(event.pointerId);
    event.preventDefault();
  });

  dock.addEventListener("pointermove", (event) => {
    if (!isDragging || (activePointerId !== null && event.pointerId !== activePointerId)) {
      return;
    }

    queuePointerFrame(event.clientX);
  });

  dock.addEventListener("pointerup", (event) => {
    if (activePointerId !== null && event.pointerId !== activePointerId) {
      return;
    }
    if (isDragging) {
      if (pointerFrameId !== null) {
        window.cancelAnimationFrame(pointerFrameId);
        pointerFrameId = null;
      }
      pendingPointerX = event.clientX;
      applyPointerPosition(event.clientX);
    }
    finishDragging();
  });

  dock.addEventListener("pointercancel", finishDragging);
  dock.addEventListener("lostpointercapture", finishDragging);

  window.addEventListener("starslix:navigate", (event) => {
    const targetScreen = event?.detail?.screen;
    if (typeof targetScreen !== "string") {
      return;
    }
    setActiveScreen(targetScreen);
    alignOrbToSelected();
  });

  window.addEventListener("starslix:dock-labels-updated", syncDockLabelGlyphs);

  syncDockLabelGlyphs();
  setActiveScreen("home");
  alignOrbToSelected();
})();

(() => {
  const grids = document.querySelectorAll(".offer-grid");
  if (!grids.length) {
    return;
  }

  const defaultUsdRubRate = 76.5;
  const defaultStarRates = {
    "50_75": 1.7,
    "76_100": 1.6,
    "101_250": 1.55,
    "251_plus": 1.5,
  };

  const getMiniappPricingConfig = () => window.starslixMiniappConfig || {};
  const getUsdRubRate = () => {
    const numeric = Number(getMiniappPricingConfig().usdRubRate);
    return Number.isFinite(numeric) && numeric > 0 ? numeric : defaultUsdRubRate;
  };
  const getStarRates = () => ({
    ...defaultStarRates,
    ...(getMiniappPricingConfig().starRates || {}),
  });
  const roundToTwo = (value) => Math.round(Number(value) * 100) / 100;

  const getStarsRateForAmount = (starsCount) => {
    const rates = getStarRates();
    if (starsCount >= 50 && starsCount <= 75) {
      return Number(rates["50_75"]) || defaultStarRates["50_75"];
    }
    if (starsCount >= 76 && starsCount <= 100) {
      return Number(rates["76_100"]) || defaultStarRates["76_100"];
    }
    if (starsCount >= 101 && starsCount <= 250) {
      return Number(rates["101_250"]) || defaultStarRates["101_250"];
    }
    return Number(rates["251_plus"]) || defaultStarRates["251_plus"];
  };

  const formatRubValue = (value) => {
    const numeric = Number(value);
    if (!Number.isFinite(numeric)) {
      return "—";
    }
    return `${Number.isInteger(numeric) ? numeric : numeric.toFixed(2)}₽`;
  };

  const formatUsdValue = (value) => {
    const numeric = Number(value);
    if (!Number.isFinite(numeric)) {
      return "—";
    }
    return `${numeric.toFixed(2)}$`;
  };

  const getPremiumPriceFromConfig = (monthsValue) => {
    const premiumOffers = Array.isArray(getMiniappPricingConfig().premium)
      ? getMiniappPricingConfig().premium
      : [];
    const match = premiumOffers.find((offer) => Number(offer?.months) === Number(monthsValue));
    if (!match) {
      return null;
    }
    return {
      usd: formatUsdValue(match.usd),
      rub: formatRubValue(match.rub),
    };
  };

  grids.forEach((grid) => {
    const cards = Array.from(grid.querySelectorAll(".offer-card"));
    const panelView = grid.closest(".panel-view");
    const quantityInput = panelView?.querySelector('.form-input[type="number"]');
    const priceUsdNode = panelView?.querySelector("[data-price-usd]");
    const priceRubNode = panelView?.querySelector("[data-price-rub]");

    const getCardAmount = (card) =>
      card.querySelector(".offer-card-amount")?.textContent?.trim() || "";

    const getCardPrice = (card) => {
      const rawPrice = card?.querySelector(".offer-card-price")?.textContent?.trim() || "";
      const [usdPart = "", rubPart = ""] = rawPrice.split("/");
      return {
        usd: usdPart.trim(),
        rub: rubPart.trim(),
      };
    };

    const syncPriceRow = (card) => {
      if (!priceUsdNode || !priceRubNode) {
        return;
      }

      const isStarsView = panelView?.dataset?.view === "stars";
      if (isStarsView && quantityInput) {
        const amount = Number.parseInt(String(quantityInput.value || "").trim(), 10);
        if (Number.isFinite(amount) && amount >= 50 && amount <= 10000) {
          const rub = roundToTwo(amount * getStarsRateForAmount(amount));
          const usd = roundToTwo(rub / getUsdRubRate());
          priceUsdNode.textContent = formatUsdValue(usd);
          priceRubNode.textContent = formatRubValue(rub);
          return;
        }
      }

      const sourceCard =
        card || cards.find((item) => item.classList.contains("is-active")) || null;
      if (!sourceCard) {
        priceUsdNode.textContent = "—";
        priceRubNode.textContent = "—";
        return;
      }

      const isPremiumView = panelView?.dataset?.view === "premium";
      if (isPremiumView) {
        const months = Number.parseInt(getCardAmount(sourceCard).replace(/[^0-9]/g, ""), 10);
        if (Number.isFinite(months)) {
          const premiumPrice = getPremiumPriceFromConfig(months);
          if (premiumPrice) {
            priceUsdNode.textContent = premiumPrice.usd;
            priceRubNode.textContent = premiumPrice.rub;
            return;
          }
        }
      }

      const { usd, rub } = getCardPrice(sourceCard);
      priceUsdNode.textContent = usd || "—";
      priceRubNode.textContent = rub || "—";
    };

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

    const syncInputWithActiveCard = () => {
      const activeCard = cards.find((item) => item.classList.contains("is-active"));
      if (quantityInput) {
        const nextValue = activeCard ? getCardAmount(activeCard) : "";
        if (quantityInput.value !== nextValue) {
          quantityInput.value = nextValue;
        }
      }

      syncPriceRow(activeCard);
    };

    const initialActiveCard = grid.querySelector(".offer-card.is-active");
    if (initialActiveCard) {
      setActiveCard(initialActiveCard);
    }
    syncInputWithActiveCard();

    const refreshGridPrices = () => {
      const activeCard = cards.find((item) => item.classList.contains("is-active")) || null;
      syncPriceRow(activeCard);
    };
    window.addEventListener("starslix:pricing-updated", refreshGridPrices);

    cards.forEach((card) => {
      card.addEventListener("click", () => {
        const shouldDeactivate = card.classList.contains("is-active");
        setActiveCard(shouldDeactivate ? null : card);
        syncInputWithActiveCard();
      });
    });

    if (quantityInput) {
      quantityInput.addEventListener("input", () => {
        const matchedCard = findCardByAmount(quantityInput.value);
        setActiveCard(matchedCard);
        syncPriceRow(matchedCard);
      });
    }
  });
})();

(() => {
  const usernameInputs = document.querySelectorAll(".form-input--telegram");
  const selfFillButtons = document.querySelectorAll(".form-self-fill-button");
  if (!usernameInputs.length && !selfFillButtons.length) {
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

  const getStoredProfileUsername = () => {
    try {
      const raw = window.localStorage.getItem("starslix:profile-user");
      const parsed = raw ? JSON.parse(raw) : null;
      if (!parsed || typeof parsed !== "object") {
        return "";
      }
      return String(parsed.username || "").trim();
    } catch {
      return "";
    }
  };

  const getCurrentTelegramUsername = () => {
    const initUsernameRaw = String(
      window.Telegram?.WebApp?.initDataUnsafe?.user?.username || ""
    ).trim();
    const initUsername = sanitizeTelegramUsername(initUsernameRaw);
    if (initUsername) {
      return initUsername;
    }

    const storedUsername = sanitizeTelegramUsername(getStoredProfileUsername());
    if (storedUsername) {
      return storedUsername;
    }

    return "";
  };

  selfFillButtons.forEach((button) => {
    button.addEventListener("click", (event) => {
      event.preventDefault();
      const wrap = button.closest(".form-input-wrap");
      const input = wrap?.querySelector(".form-input--telegram");
      if (!(input instanceof HTMLInputElement)) {
        return;
      }

      const usernameValue = getCurrentTelegramUsername();
      if (!usernameValue) {
        return;
      }

      input.value = usernameValue;
      input.dispatchEvent(new Event("input", { bubbles: true }));
      input.dispatchEvent(new Event("change", { bubbles: true }));
      input.focus({ preventScroll: true });
    });
  });
})();

(() => {
  const dismissKeyboardIfNeeded = (event) => {
    const activeElement = document.activeElement;
    if (!activeElement) {
      return;
    }

    const isTextInput =
      activeElement.tagName === "INPUT" || activeElement.tagName === "TEXTAREA";
    if (!isTextInput) {
      return;
    }

    const target = event.target;
    if (!(target instanceof Element)) {
      return;
    }

    if (target.closest(".form-input-wrap")) {
      return;
    }

    activeElement.blur();
  };

  document.addEventListener("pointerdown", dismissKeyboardIfNeeded);
  document.addEventListener("touchstart", dismissKeyboardIfNeeded, { passive: true });
})();

(() => {
  const dock = document.getElementById("appDock");
  if (!dock) {
    return;
  }

  const isFormInput = (node) =>
    node instanceof HTMLInputElement || node instanceof HTMLTextAreaElement;

  const updateDockVisibilityForInput = () => {
    const activeElement = document.activeElement;
    const shouldHide =
      isFormInput(activeElement) &&
      activeElement.closest(".main-glass-panel, .panel-view, .form-stack");

    dock.classList.toggle("is-input-hidden", Boolean(shouldHide));
  };

  document.addEventListener("focusin", updateDockVisibilityForInput);
  document.addEventListener("focusout", () => {
    window.setTimeout(updateDockVisibilityForInput, 0);
  });

  window.addEventListener("starslix:navigate", () => {
    window.setTimeout(updateDockVisibilityForInput, 0);
  });
})();

(() => {
  const avatarToggle = document.getElementById("userAvatarToggle");
  const quickMenu = document.getElementById("headerQuickMenu");
  const storePage = document.getElementById("storePage");
  const profilePage = document.getElementById("profilePage");
  const profileCloseButton = document.getElementById("profileCloseButton");
  const profileAvatar = document.getElementById("profileAvatar");
  const profileFullname = document.getElementById("profileFullname");
  const profileUsername = document.getElementById("profileUsername");
  const profileTelegramId = document.getElementById("profileTelegramId");
  const profileHistoryList = document.getElementById("profileHistoryList");
  const profileHistoryEmpty = document.getElementById("profileHistoryEmpty");
  const profileHistoryControls = document.getElementById("profileHistoryControls");
  const profileHistoryPrev = document.getElementById("profileHistoryPrev");
  const profileHistoryNext = document.getElementById("profileHistoryNext");
  const profileHistoryPage = document.getElementById("profileHistoryPage");
  const profileAdminActions = document.getElementById("profileAdminActions");
  const profileAdminButton = document.getElementById("profileAdminButton");
  const profileAdminPanel = document.getElementById("profileAdminPanel");
  const profileAdminSaveButton = document.getElementById("profileAdminSaveButton");
  const adminRateInputs = {
    "50_75": document.getElementById("adminRate50_75"),
    "76_100": document.getElementById("adminRate76_100"),
    "101_250": document.getElementById("adminRate101_250"),
    "251_plus": document.getElementById("adminRate251_plus"),
  };
  const appAvatarNodes = document.querySelectorAll(
    ".user-avatar, #profileAvatar, .app-dock-button[data-target=\"profile\"] .app-dock-icon--avatar"
  );
  if (!storePage || !profilePage) {
    return;
  }

  const historyStorageKey = "starslix_purchase_history";
  const historyPageSize = 8;
  let historyPage = 0;
  const historyTypeMeta = {
    stars: {
      icon: "zvezda.png",
    },
    premium: {
      icon: "blackpremicon.png",
    },
    ton: {
      icon: "blactonicon.png",
    },
  };

  const setProfilePageState = (isProfilePage) => {
    document.body.classList.toggle("is-profile-page", isProfilePage);
    if (avatarToggle) {
      avatarToggle.setAttribute("aria-expanded", isProfilePage ? "true" : "false");
    }
    profilePage.setAttribute("aria-hidden", isProfilePage ? "false" : "true");
    storePage.setAttribute("aria-hidden", isProfilePage ? "true" : "false");
  };

  const formatHistoryDate = (isoValue) => {
    const parsedDate = new Date(isoValue);
    if (Number.isNaN(parsedDate.getTime())) {
      return "";
    }
    return parsedDate.toLocaleString();
  };

  const loadHistory = () => {
    try {
      const rawValue = window.localStorage.getItem(historyStorageKey);
      const parsed = rawValue ? JSON.parse(rawValue) : [];
      return Array.isArray(parsed) ? parsed : [];
    } catch {
      return [];
    }
  };

  const saveHistory = (items) => {
    try {
      window.localStorage.setItem(historyStorageKey, JSON.stringify(items));
    } catch {
      // ignore write errors
    }
  };

  let historyItems = loadHistory();

  const normalizeHistoryType = (item) => {
    const rawType = String(item?.type || "").trim().toLowerCase();
    if (rawType === "stars" || rawType === "premium" || rawType === "ton") {
      return rawType;
    }

    const hint = String(item?.main || "").trim().toLowerCase();
    if (hint.includes("ton")) {
      return "ton";
    }
    if (hint.includes("прем") || hint.includes("premium") || hint.includes("мес")) {
      return "premium";
    }
    return "stars";
  };

  const normalizeHistoryMain = (item, type) => {
    const rawMain = String(item?.main || "").trim();
    if (!rawMain) {
      return "—";
    }

    if (type === "stars") {
      return rawMain.replace(/^(зв[её]зды?|stars?)\s*[:\-]?\s*/i, "").trim() || "—";
    }
    if (type === "premium") {
      return rawMain.replace(/^(премиум|premium)\s*[:\-]?\s*/i, "").trim() || "—";
    }
    return rawMain.replace(/^(ton)\s*[:\-]?\s*/i, "").trim() || "—";
  };

  const updateHistoryPaginationUi = (totalPages) => {
    if (!profileHistoryControls || !profileHistoryPrev || !profileHistoryNext || !profileHistoryPage) {
      return;
    }

    const safePages = Math.max(1, totalPages);
    profileHistoryControls.hidden = safePages <= 1;
    profileHistoryPrev.disabled = historyPage <= 0;
    profileHistoryNext.disabled = historyPage >= safePages - 1;
    profileHistoryPage.textContent = `${historyPage + 1} / ${safePages}`;
  };

  const renderHistory = () => {
    if (!profileHistoryList || !profileHistoryEmpty) {
      return;
    }

    profileHistoryList.querySelectorAll(".profile-history-item").forEach((node) => node.remove());

    if (!historyItems.length) {
      profileHistoryEmpty.hidden = false;
      updateHistoryPaginationUi(1);
      return;
    }

    profileHistoryEmpty.hidden = true;

    const totalPages = Math.max(1, Math.ceil(historyItems.length / historyPageSize));
    historyPage = Math.min(Math.max(0, historyPage), totalPages - 1);
    const pageStart = historyPage * historyPageSize;
    const visibleItems = historyItems.slice(pageStart, pageStart + historyPageSize);

    visibleItems.forEach((item) => {
      const type = normalizeHistoryType(item);
      const iconSrc = historyTypeMeta[type]?.icon || historyTypeMeta.stars.icon;

      const row = document.createElement("div");
      row.className = `profile-history-item profile-history-item--${type}`;

      const iconWrap = document.createElement("span");
      iconWrap.className = "profile-history-icon-wrap";
      iconWrap.setAttribute("aria-hidden", "true");

      const icon = document.createElement("img");
      icon.className = "profile-history-icon";
      icon.src = iconSrc;
      icon.alt = "";
      iconWrap.appendChild(icon);

      const main = document.createElement("span");
      main.className = "profile-history-main";
      main.textContent = normalizeHistoryMain(item, type);

      const price = document.createElement("span");
      price.className = "profile-history-price";
      price.textContent = item.price || "—";

      const date = document.createElement("span");
      date.className = "profile-history-date";
      date.textContent = formatHistoryDate(item.createdAt);

      row.append(iconWrap, main, price, date);
      profileHistoryList.appendChild(row);
    });

    updateHistoryPaginationUi(totalPages);
  };

  const addCurrentOrderToHistory = (override = null) => {
    const activeView = document.querySelector(".panel-view.is-active");
    if (!activeView) {
      return;
    }

    const activeCard =
      activeView.querySelector(".offer-card.is-active") || activeView.querySelector(".offer-card");
    if (!activeCard) {
      return;
    }

    const viewName = activeView.dataset.view || "";
    const purchaseType =
      viewName === "premium" || viewName === "ton" ? viewName : "stars";
    const priceUsdText = activeView.querySelector("[data-price-usd]")?.textContent?.trim() || "";
    const priceRubText = activeView.querySelector("[data-price-rub]")?.textContent?.trim() || "";

    const formatRubHistory = (value) => {
      const numeric = Number(value);
      if (!Number.isFinite(numeric)) {
        return "";
      }
      return `${Number.isInteger(numeric) ? numeric : numeric.toFixed(2)}₽`;
    };

    const formatUsdHistory = (value) => {
      const numeric = Number(value);
      if (!Number.isFinite(numeric)) {
        return "";
      }
      return `${numeric.toFixed(2)}$`;
    };

    const cardAmountText = activeCard.querySelector(".offer-card-amount")?.textContent?.trim() || "";
    let amountText = cardAmountText;

    if (purchaseType === "stars") {
      const starsInput = activeView.querySelector('.form-input[type="number"]');
      const starsInputValue = String(starsInput?.value || "").trim();
      if (starsInputValue) {
        amountText = starsInputValue;
      }
    }

    if (override?.amount) {
      if (purchaseType === "premium") {
        amountText = `${override.amount} мес.`;
      } else {
        amountText = String(override.amount);
      }
    }

    let priceText = activeCard.querySelector(".offer-card-price")?.textContent?.trim() || "—";
    if (priceUsdText && priceRubText) {
      priceText = `${priceUsdText}/${priceRubText}`;
    }
    if (
      override &&
      override.priceUsd !== undefined &&
      override.priceRub !== undefined
    ) {
      const usd = formatUsdHistory(override.priceUsd);
      const rub = formatRubHistory(override.priceRub);
      priceText = usd && rub ? `${usd}/${rub}` : priceText;
    }

    historyItems.unshift({
      type: purchaseType,
      main: amountText || "—",
      price: priceText,
      createdAt: new Date().toISOString(),
    });
    historyItems = historyItems.slice(0, 20);

    saveHistory(historyItems);
    historyPage = 0;
    renderHistory();
  };

  const apiMeta = document.querySelector('meta[name="starslix-api-base"]');
  const apiBaseRaw = (apiMeta?.content || "").trim().replace(/\/+$/, "");
  const isMixedContentBlocked =
    window.location.protocol === "https:" && /^http:\/\//i.test(apiBaseRaw);
  const apiBase = isMixedContentBlocked ? "" : apiBaseRaw;
  const buildApiUrl = (path) => (apiBase ? `${apiBase}${path}` : path);
  const profileStorageKey = "starslix:profile-user";
  let currentUserIsAdmin = false;
  let adminPanelLoaded = false;

  const showMiniappMessage = (text) => {
    if (window.Telegram?.WebApp?.showAlert) {
      window.Telegram.WebApp.showAlert(text);
      return;
    }
    window.alert(text);
  };

  const setAdminPanelVisibility = (isAdmin) => {
    currentUserIsAdmin = Boolean(isAdmin);
    if (profileAdminActions) {
      profileAdminActions.hidden = !currentUserIsAdmin;
    }
    if (profileAdminButton) {
      profileAdminButton.disabled = !currentUserIsAdmin;
      profileAdminButton.setAttribute("aria-hidden", currentUserIsAdmin ? "false" : "true");
      if (profileAdminButton.dataset.textOpen) {
        profileAdminButton.textContent = profileAdminButton.dataset.textOpen;
      }
    }
    if (profileAdminPanel) {
      profileAdminPanel.hidden = true;
    }
    if (!currentUserIsAdmin) {
      adminPanelLoaded = false;
    }
  };

  const formatRateValue = (value) => {
    const numeric = Number(value);
    if (!Number.isFinite(numeric) || numeric <= 0) {
      return "";
    }
    return String(Number(numeric.toFixed(3)));
  };

  const collectAdminRatesPayload = () => {
    const payload = {};
    for (const [key, input] of Object.entries(adminRateInputs)) {
      const numeric = Number(String(input?.value || "").replace(",", ".").trim());
      if (!Number.isFinite(numeric) || numeric <= 0) {
        return null;
      }
      payload[key] = Number(numeric.toFixed(3));
    }
    return payload;
  };

  const applyAdminRatesToInputs = (rates) => {
    if (!rates || typeof rates !== "object") {
      return;
    }
    for (const [key, input] of Object.entries(adminRateInputs)) {
      if (!(input instanceof HTMLInputElement)) {
        continue;
      }
      input.value = formatRateValue(rates[key]);
    }
  };

  const fetchAdminRates = async () => {
    const initData = window.Telegram?.WebApp?.initData || "";
    if (!initData) {
      showMiniappMessage("Открой мини-приложение через Telegram");
      return false;
    }

    try {
      const response = await fetch(buildApiUrl("/api/miniapp/admin/rates"), {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Accept: "application/json",
        },
        body: JSON.stringify({ initData }),
      });
      const data = await response.json().catch(() => ({}));
      if (!response.ok || !data?.ok) {
        showMiniappMessage(data?.error || "Не удалось загрузить админ-настройки");
        return false;
      }
      applyAdminRatesToInputs(data.starRates || {});
      adminPanelLoaded = true;
      return true;
    } catch {
      showMiniappMessage("Ошибка сети при загрузке админ-настроек");
      return false;
    }
  };

  const parseUserFromInitData = (initDataRaw) => {
    if (!initDataRaw) {
      return null;
    }
    try {
      const params = new URLSearchParams(initDataRaw);
      const rawUser = params.get("user");
      if (!rawUser) {
        return null;
      }
      return JSON.parse(rawUser);
    } catch {
      return null;
    }
  };

  const normalizeProfileUser = (rawUser) => {
    if (!rawUser || typeof rawUser !== "object") {
      return null;
    }
    const firstName = String(rawUser.first_name ?? rawUser.firstName ?? "").trim();
    const lastName = String(rawUser.last_name ?? rawUser.lastName ?? "").trim();
    const usernameRaw = String(rawUser.username ?? "").trim().replace(/^@+/, "");
    const username = usernameRaw ? `@${usernameRaw}` : "";
    const telegramId = rawUser.id ? String(rawUser.id) : "";
    const fullName = String(rawUser.fullName ?? "").trim() || [firstName, lastName].filter(Boolean).join(" ").trim();
    const photoUrl = String(rawUser.photo_url ?? rawUser.photoUrl ?? "").trim();
    return {
      firstName,
      lastName,
      fullName,
      username,
      telegramId,
      photoUrl,
    };
  };

  const loadStoredProfileUser = () => {
    try {
      const raw = window.localStorage.getItem(profileStorageKey);
      return raw ? JSON.parse(raw) : null;
    } catch {
      return null;
    }
  };

  const saveStoredProfileUser = (profileUser) => {
    if (!profileUser) {
      return;
    }
    try {
      window.localStorage.setItem(profileStorageKey, JSON.stringify(profileUser));
    } catch {
      // ignore storage write errors
    }
  };

  const mergeProfileUsers = (baseUser, nextUser) => {
    const base = baseUser || {};
    const next = nextUser || {};
    return {
      firstName: next.firstName || base.firstName || "",
      lastName: next.lastName || base.lastName || "",
      fullName: next.fullName || base.fullName || "",
      username: next.username || base.username || "",
      telegramId: next.telegramId || base.telegramId || "",
      photoUrl: next.photoUrl || base.photoUrl || "",
    };
  };

  const applyProfileFields = (profileUser) => {
    const user = profileUser || {};
    if (profileUsername) {
      profileUsername.textContent = user.username || "—";
    }
    if (profileTelegramId) {
      profileTelegramId.textContent = user.telegramId || "—";
    }
    if (profileFullname) {
      const fullName = user.fullName || [user.firstName, user.lastName].filter(Boolean).join(" ").trim();
      if (fullName) {
        profileFullname.textContent = fullName;
        profileFullname.dataset.empty = "false";
      }
    }
  };

  const webAppUser = normalizeProfileUser(
    window.Telegram?.WebApp?.initDataUnsafe?.user ||
      parseUserFromInitData(window.Telegram?.WebApp?.initData || "")
  );
  const storedProfileUser = normalizeProfileUser(loadStoredProfileUser());
  let currentProfileUser = mergeProfileUsers(storedProfileUser, webAppUser);

  applyProfileFields(currentProfileUser);
  setAdminPanelVisibility(false);
  if (webAppUser) {
    saveStoredProfileUser(currentProfileUser);
  }

  const applyAvatarSrc = (src) => {
    appAvatarNodes.forEach((avatarNode) => {
      if (!(avatarNode instanceof HTMLImageElement)) {
        return;
      }
      const fallbackSrc =
        avatarNode.dataset.fallbackSrc ||
        avatarNode.getAttribute("src") ||
        "avauser.png";
      avatarNode.dataset.fallbackSrc = fallbackSrc;
      avatarNode.onerror = () => {
        if (avatarNode.getAttribute("src") !== fallbackSrc) {
          avatarNode.src = fallbackSrc;
        }
      };
      avatarNode.src = src || fallbackSrc;
    });
  };

  const avatarStorageKey = "starslix:avatar-url";
  let storedAvatarUrl = "";
  try {
    storedAvatarUrl = window.localStorage.getItem(avatarStorageKey) || "";
  } catch (error) {}

  const currentAvatarUrl =
    typeof currentProfileUser?.photoUrl === "string" ? currentProfileUser.photoUrl.trim() : "";

  if (currentAvatarUrl) {
    try {
      window.localStorage.setItem(avatarStorageKey, currentAvatarUrl);
    } catch (error) {}
  }

  applyAvatarSrc(currentAvatarUrl || storedAvatarUrl || "");

  const applyHistoryFromApi = (rawHistory) => {
    if (!Array.isArray(rawHistory)) {
      return;
    }
    historyItems = rawHistory
      .map((item) => ({
        type: normalizeHistoryType(item),
        main: String(item?.main || "").trim() || "—",
        price: String(item?.price || "").trim() || "—",
        createdAt: item?.createdAt || new Date().toISOString(),
      }))
      .slice(0, 20);
    saveHistory(historyItems);
    historyPage = 0;
    renderHistory();
  };

  const fetchProfileFromApi = async () => {
    const initData = window.Telegram?.WebApp?.initData || "";
    if (!initData) {
      return;
    }

    try {
      const response = await fetch(buildApiUrl("/api/miniapp/profile"), {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Accept: "application/json",
        },
        body: JSON.stringify({ initData }),
      });

      if (!response.ok) {
        return;
      }

      const data = await response.json().catch(() => null);
      if (!data?.ok) {
        return;
      }

      setAdminPanelVisibility(Boolean(data.isAdmin));

      const apiUser = normalizeProfileUser(data.user || {});
      if (apiUser) {
        currentProfileUser = mergeProfileUsers(currentProfileUser, apiUser);
        applyProfileFields(currentProfileUser);
        saveStoredProfileUser(currentProfileUser);

        if (currentProfileUser.photoUrl) {
          try {
            window.localStorage.setItem(avatarStorageKey, currentProfileUser.photoUrl);
          } catch {
            // ignore storage write errors
          }
          applyAvatarSrc(currentProfileUser.photoUrl);
        }
      }

      applyHistoryFromApi(data.history);
    } catch {
      // silent fallback to cached local profile/history
    }
  };

  if (avatarToggle) {
    avatarToggle.removeAttribute("disabled");
    avatarToggle.removeAttribute("aria-disabled");
    avatarToggle.type = "button";
    avatarToggle.setAttribute("aria-expanded", "false");
  }
  if (quickMenu) {
    quickMenu.classList.add("is-open");
    quickMenu.setAttribute("aria-hidden", "false");
  }

  avatarToggle?.addEventListener("click", (event) => {
    event.preventDefault();
    window.dispatchEvent(
      new CustomEvent("starslix:navigate", {
        detail: { screen: "profile" },
      })
    );
  });

  profileCloseButton?.addEventListener("click", (event) => {
    event.preventDefault();
    window.dispatchEvent(
      new CustomEvent("starslix:navigate", {
        detail: { screen: "home" },
      })
    );
  });

  profileHistoryPrev?.addEventListener("click", (event) => {
    event.preventDefault();
    if (historyPage <= 0) {
      return;
    }
    historyPage -= 1;
    renderHistory();
  });

  profileHistoryNext?.addEventListener("click", (event) => {
    event.preventDefault();
    const totalPages = Math.max(1, Math.ceil(historyItems.length / historyPageSize));
    if (historyPage >= totalPages - 1) {
      return;
    }
    historyPage += 1;
    renderHistory();
  });

  profileAdminButton?.addEventListener("click", async (event) => {
    event.preventDefault();
    if (!currentUserIsAdmin) {
      return;
    }

    if (!profileAdminPanel) {
      return;
    }

    if (profileAdminPanel.hidden) {
      if (!adminPanelLoaded) {
        profileAdminButton.disabled = true;
        const loaded = await fetchAdminRates();
        profileAdminButton.disabled = !currentUserIsAdmin;
        if (!loaded) {
          return;
        }
      }
      profileAdminPanel.hidden = false;
      profileAdminButton.textContent =
        profileAdminButton.dataset.textHide || profileAdminButton.textContent;
    } else {
      profileAdminPanel.hidden = true;
      profileAdminButton.textContent =
        profileAdminButton.dataset.textOpen || profileAdminButton.textContent;
    }
  });

  profileAdminSaveButton?.addEventListener("click", async (event) => {
    event.preventDefault();
    if (!currentUserIsAdmin) {
      return;
    }

    const ratesPayload = collectAdminRatesPayload();
    if (!ratesPayload) {
      showMiniappMessage("Введите корректные тарифы больше нуля");
      return;
    }

    const initData = window.Telegram?.WebApp?.initData || "";
    if (!initData) {
      showMiniappMessage("Открой мини-приложение через Telegram");
      return;
    }

    profileAdminSaveButton.disabled = true;
    try {
      const response = await fetch(buildApiUrl("/api/miniapp/admin/rates"), {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Accept: "application/json",
        },
        body: JSON.stringify({
          initData,
          starRates: ratesPayload,
        }),
      });
      const data = await response.json().catch(() => ({}));
      if (!response.ok || !data?.ok) {
        showMiniappMessage(data?.error || "Не удалось сохранить тарифы");
        return;
      }

      applyAdminRatesToInputs(data.starRates || ratesPayload);
      window.dispatchEvent(new CustomEvent("starslix:pricing-updated"));
      showMiniappMessage("Тарифы сохранены");
    } catch {
      showMiniappMessage("Ошибка сети при сохранении тарифов");
    } finally {
      profileAdminSaveButton.disabled = false;
    }
  });

  // Экспортируем, чтобы добавлять запись после успешной отправки заказа на backend.
  window.starslixAddCurrentOrderToHistory = addCurrentOrderToHistory;

  setProfilePageState(false);
  renderHistory();
  fetchProfileFromApi();
})();

(() => {
  const telegramBotButton = document.getElementById("telegramBotButton");
  if (!telegramBotButton) {
    return;
  }

  const telegramBotUrl = "https://t.me/starslixbot";

  telegramBotButton.addEventListener("click", (event) => {
    event.preventDefault();

    if (window.Telegram?.WebApp?.openTelegramLink) {
      window.Telegram.WebApp.openTelegramLink(telegramBotUrl);
      return;
    }

    window.open(telegramBotUrl, "_blank", "noopener,noreferrer");
  });
})();

(() => {
  const apiMeta = document.querySelector('meta[name="starslix-api-base"]');
  const apiBaseRaw = (apiMeta?.content || "").trim().replace(/\/+$/, "");
  const isMixedContentBlocked =
    window.location.protocol === "https:" && /^http:\/\//i.test(apiBaseRaw);
  const apiBase = isMixedContentBlocked ? "" : apiBaseRaw;
  const buildApiUrl = (path) => (apiBase ? `${apiBase}${path}` : path);

  const showMessage = (text) => {
    if (window.Telegram?.WebApp?.showAlert) {
      window.Telegram.WebApp.showAlert(text);
      return;
    }
    window.alert(text);
  };

  const formatRub = (value) => {
    const numeric = Number(value);
    if (!Number.isFinite(numeric)) {
      return "—";
    }
    return `${Number.isInteger(numeric) ? numeric : numeric.toFixed(2)}₽`;
  };

  const formatUsd = (value) => {
    const numeric = Number(value);
    if (!Number.isFinite(numeric)) {
      return "—";
    }
    return `${numeric.toFixed(2)}$`;
  };

  const syncPanelPriceRow = (gridNode) => {
    const panelView = gridNode?.closest(".panel-view");
    if (!panelView) {
      return;
    }
    const priceUsdNode = panelView.querySelector("[data-price-usd]");
    const priceRubNode = panelView.querySelector("[data-price-rub]");
    if (!priceUsdNode || !priceRubNode) {
      return;
    }

    const sourceCard =
      gridNode.querySelector(".offer-card.is-active");
    if (!sourceCard) {
      priceUsdNode.textContent = "—";
      priceRubNode.textContent = "—";
      return;
    }

    const rawPrice = sourceCard.querySelector(".offer-card-price")?.textContent?.trim() || "";
    const [usdPart = "—", rubPart = "—"] = rawPrice.split("/");
    priceUsdNode.textContent = usdPart.trim() || "—";
    priceRubNode.textContent = rubPart.trim() || "—";
  };

  const applyConfigToCards = (config) => {
    window.starslixMiniappConfig = {
      ...(window.starslixMiniappConfig || {}),
      ...(config || {}),
    };

    const starsCards = Array.from(document.querySelectorAll(".offer-grid-stars .offer-card"));
    (config?.stars || []).slice(0, starsCards.length).forEach((offer, index) => {
      const card = starsCards[index];
      const amountNode = card.querySelector(".offer-card-amount");
      const priceNode = card.querySelector(".offer-card-price");
      if (amountNode) {
        amountNode.textContent = String(offer.amount);
      }
      if (priceNode) {
        priceNode.textContent = `${formatUsd(offer.usd)}/${formatRub(offer.rub)}`;
      }
      card.dataset.amountValue = String(offer.amount);
    });

    const premiumCards = Array.from(document.querySelectorAll(".offer-grid-premium .offer-card"));
    (config?.premium || []).slice(0, premiumCards.length).forEach((offer, index) => {
      const card = premiumCards[index];
      const priceNode = card.querySelector(".offer-card-price");
      if (priceNode) {
        priceNode.textContent = `${formatUsd(offer.usd)}/${formatRub(offer.rub)}`;
      }
      card.dataset.amountValue = String(offer.months);
    });

    document.querySelectorAll(".offer-grid").forEach(syncPanelPriceRow);
    window.dispatchEvent(new CustomEvent("starslix:pricing-updated"));
  };

  const fetchMiniappConfig = async () => {
    try {
      const response = await fetch(buildApiUrl("/api/miniapp/config"), {
        method: "GET",
        headers: {
          Accept: "application/json",
        },
      });
      if (!response.ok) {
        return;
      }

      const data = await response.json();
      if (data?.ok) {
        applyConfigToCards(data);
      }
    } catch (error) {}
  };

  const clearPanelInputs = (panelView) => {
    if (!panelView) {
      return;
    }

    panelView.querySelectorAll(".form-input").forEach((inputNode) => {
      if (!(inputNode instanceof HTMLInputElement || inputNode instanceof HTMLTextAreaElement)) {
        return;
      }
      inputNode.value = "";
      inputNode.dispatchEvent(new Event("input", { bubbles: true }));
      inputNode.dispatchEvent(new Event("change", { bubbles: true }));
    });

    const activeElement = document.activeElement;
    if (activeElement instanceof HTMLElement && panelView.contains(activeElement)) {
      activeElement.blur();
    }
  };

  const parseAmountFromCard = (card) => {
    if (!card) {
      return "";
    }
    if (card.dataset.amountValue) {
      return card.dataset.amountValue;
    }
    const amountText = card.querySelector(".offer-card-amount")?.textContent?.trim() || "";
    return amountText.replace(/[^0-9.]/g, "");
  };

  const getOrderPayload = (panelView) => {
    const type = panelView?.dataset?.view;
    if (type !== "stars" && type !== "premium") {
      return null;
    }

    const usernameInput = panelView.querySelector(".form-input--telegram");
    const promoInput = panelView.querySelector(".form-input--promo");
    const username = usernameInput?.value?.trim() || "";
    const promoCode = promoInput?.value?.trim() || "";

    let amount = "";
    if (type === "stars") {
      const amountInput = panelView.querySelector('.form-input[type="number"]');
      const selectedCard =
        panelView.querySelector(".offer-card.is-active") ||
        panelView.querySelector(".offer-grid-stars .offer-card");
      amount = (amountInput?.value || "").trim() || parseAmountFromCard(selectedCard);
    } else {
      const selectedCard =
        panelView.querySelector(".offer-card.is-active") ||
        panelView.querySelector(".offer-grid-premium .offer-card");
      amount = parseAmountFromCard(selectedCard);
    }

    if (!username) {
      return { error: "Введите Telegram username" };
    }
    if (!amount) {
      return { error: type === "stars" ? "Введите количество звёзд" : "Выберите пакет Premium" };
    }

    const initData = window.Telegram?.WebApp?.initData || "";
    const hasSendData = typeof window.Telegram?.WebApp?.sendData === "function";
    // Even with empty meta base we can call same-origin `/api/miniapp/...`.
    const shouldUseApi = true;
    if (shouldUseApi && !initData) {
      return { error: "Открой мини-приложение через Telegram" };
    }
    if (!shouldUseApi && !initData && !hasSendData) {
      return { error: "Открой мини-приложение через Telegram" };
    }

    return {
      type,
      username,
      amount,
      promoCode,
      initData,
    };
  };

  const submitOrder = async (button, panelView) => {
    const payload = getOrderPayload(panelView);
    if (!payload || payload.error) {
      showMessage(payload?.error || "Некорректные данные заказа");
      return;
    }

    const parsePriceFromNode = (node) => {
      const rawValue = String(node?.textContent || "").trim();
      if (!rawValue || rawValue === "—") {
        return null;
      }
      const normalized = rawValue.replace(",", ".").replace(/[^0-9.\-]/g, "");
      const numeric = Number.parseFloat(normalized);
      return Number.isFinite(numeric) ? numeric : null;
    };

    const currentPriceUsd = parsePriceFromNode(panelView.querySelector("[data-price-usd]"));
    const currentPriceRub = parsePriceFromNode(panelView.querySelector("[data-price-rub]"));

    const trySendViaWebAppData = () => {
      if (!window.Telegram?.WebApp?.sendData) {
        return false;
      }

      window.Telegram.WebApp.sendData(
        JSON.stringify({
          source: "miniapp",
          type: payload.type,
          username: payload.username,
          amount: payload.amount,
          promoCode: payload.promoCode,
          clientPrice: {
            usd: currentPriceUsd,
            rub: currentPriceRub,
          },
        })
      );

      if (typeof window.starslixAddCurrentOrderToHistory === "function") {
        window.starslixAddCurrentOrderToHistory({
          type: payload.type,
          amount: payload.amount,
          priceUsd: currentPriceUsd,
          priceRub: currentPriceRub,
        });
      }

      clearPanelInputs(panelView);
      showMessage("Запрос отправлен в Telegram. Если бот не ответил в чате, открой Mini App через кнопку в чате бота или включи HTTPS API.");
      return true;
    };

    button.disabled = true;
    button.classList.add("is-loading");

    try {
      const response = await fetch(buildApiUrl("/api/miniapp/order"), {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Accept: "application/json",
        },
        body: JSON.stringify(payload),
      });
      const data = await response.json().catch(() => ({}));

      if (!response.ok || !data?.ok) {
        if (trySendViaWebAppData()) {
          return;
        }
        const details = data?.error || `Ошибка API (${response.status})`;
        showMessage(`Не удалось отправить заявку: ${details}`);
        return;
      }

      if (typeof window.starslixAddCurrentOrderToHistory === "function") {
        window.starslixAddCurrentOrderToHistory({
          type: payload.type,
          amount: payload.amount,
          priceUsd: data?.price?.usd,
          priceRub: data?.price?.rub,
        });
      }

      clearPanelInputs(panelView);
      const promoError = data?.promo?.error ? `\nПромокод: ${data.promo.error}` : "";
      showMessage(`Заявка отправлена в группу${promoError}`);
      return;
    } catch (error) {
      try {
        if (trySendViaWebAppData()) {
          return;
        }
      } catch (fallbackError) {}
      showMessage(
        `Ошибка сети при отправке заявки: ${error?.message || "unknown error"}`
      );
    } finally {
      button.disabled = false;
      button.classList.remove("is-loading");
    }
  };

  document
    .querySelectorAll(".form-stack--stars .order-submit-button, .form-stack--premium .order-submit-button")
    .forEach((button) => {
      button.addEventListener("click", (event) => {
        event.preventDefault();
        const panelView = button.closest(".panel-view");
        if (!panelView) {
          return;
        }
        submitOrder(button, panelView);
      });
    });

  document.querySelectorAll(".form-stack--ton .order-submit-button").forEach((button) => {
    button.addEventListener("click", (event) => {
      event.preventDefault();
      const panelView = button.closest(".panel-view");
      clearPanelInputs(panelView);
    });
  });

  fetchMiniappConfig();
})();

(() => {
  const languageToggleButton = document.getElementById("languageToggleButton");
  const languageDropdown = document.getElementById("languageDropdown");
  const languageSwitch = document.getElementById("headerLanguageSwitch");
  const languageOptions = Array.from(document.querySelectorAll(".header-language-option"));
  if (!languageToggleButton || !languageDropdown || !languageSwitch || !languageOptions.length) {
    return;
  }

  const translations = {
    ru: {
      loaderAria: "Загрузка",
      profileButtonAria: "Открыть страницу профиля",
      telegramButton: "Telegram-бот",
      languageButton: "Смена языка",
      languageShort: "RU",
      languageOptionRu: "Русский",
      languageOptionEn: "Английский",
      heroMobileLine1: "Получите",
      heroMobileLine2Prefix: "свои ",
      heroStars: "звёзды",
      heroMobileLine3: "здесь!",
      heroWideLine1: "Получите свои",
      heroWideLine2Suffix: "здесь!",
      heroSubtitleLine1: "Покупайте звёзды быстро и анонимно.",
      heroSubtitleLine2: "Никаких KYC верификации и риска возврата.",
      topPillAria: "Улучшите свои впечатления от Telegram",
      topPillText: "Улучшите свои впечатления от Telegram",
      dockAria: "Нижнее меню",
      dockHome: "Главная",
      dockLeaders: "Лидеры",
      dockContest: "Конкурс",
      dockProfile: "Профиль",
      leadersPageAria: "Страница лидеров",
      leadersTitle: "Лидеры",
      leadersText: "Раздел лидеров в разработке.",
      contestPageAria: "Страница конкурса",
      contestTitle: "Конкурс",
      contestText: "Раздел конкурса в разработке.",
      mainPanelAria: "Основной блок контента",
      panelTabsAria: "Разделы",
      panelTabStarsAria: "Звёзды",
      panelTabPremiumAria: "Премиум",
      starsGridAria: "Пакеты звёзд",
      premiumGridAria: "Пакеты премиума",
      cardStars100Aria: "Пакет звёзд 100",
      cardStars500Aria: "Пакет звёзд 500",
      cardStars1000Aria: "Пакет звёзд 1000",
      cardPremium100Aria: "Пакет премиума 3 месяца",
      cardPremium500Aria: "Пакет премиума 6 месяцев",
      cardPremium1000Aria: "Пакет премиума 12 месяцев",
      premiumDuration3: "3 мес.",
      premiumDuration6: "6 мес.",
      premiumDuration12: "12 мес.",
      badgeValue: "Выгодно",
      badgePopular: "Популярно",
      usernameFloatingLabel: "Введите Telegram @username",
      selfFillButton: "Себе",
      amountFloatingLabel: "Введите количество (50 - 10000)",
      tonAmountFloatingLabel: "Введите количество TON",
      promoFloatingLabel: "Введите промокод",
      paymentMethodLabel: "Метод оплаты",
      paymentMethodText: "СПБ #1 (PALLY)",
      orderButton: "Оформить заказ",
      tonOrderButton: "Отправить TON",
      orderNote:
        "Приобретая Telegram Stars или Telegram Premium, вы подтверждаете свое согласие с нашими Условиями и подтверждаете, что не являетесь резидентом США.",
      profilePageAria: "Страница профиля",
      profileCloseAria: "Закрыть профиль",
      profileFallbackName: "Пользователь Telegram",
      profileLabelUsername: "Telegram username",
      profileLabelTelegramId: "Telegram ID",
      profileHistoryTitle: "История покупок",
      profileHistoryEmpty: "Покупок пока нет",
      profileAdminButton: "Админ-панель",
      profileAdminButtonHide: "Скрыть админ-панель",
      profileAdminPanelTitle: "Тарифы звёзд",
      profileAdminSaveButton: "Сохранить тарифы",
    },
    en: {
      loaderAria: "Loading",
      profileButtonAria: "Open profile page",
      telegramButton: "Telegram bot",
      languageButton: "Switch language",
      languageShort: "EN",
      languageOptionRu: "Russian",
      languageOptionEn: "English",
      heroMobileLine1: "Get",
      heroMobileLine2Prefix: "your ",
      heroStars: "stars",
      heroMobileLine3: "here!",
      heroWideLine1: "Get your",
      heroWideLine2Suffix: "here!",
      heroSubtitleLine1: "Buy stars quickly and anonymously.",
      heroSubtitleLine2: "No KYC verification and no chargeback risk.",
      topPillAria: "Enhance your Telegram experience",
      topPillText: "Enhance your Telegram experience",
      dockAria: "Bottom menu",
      dockHome: "Home",
      dockLeaders: "Leaders",
      dockContest: "Contest",
      dockProfile: "Profile",
      leadersPageAria: "Leaders page",
      leadersTitle: "Leaders",
      leadersText: "Leaders section is in progress.",
      contestPageAria: "Contest page",
      contestTitle: "Contest",
      contestText: "Contest section is in progress.",
      mainPanelAria: "Main content section",
      panelTabsAria: "Sections",
      panelTabStarsAria: "Stars",
      panelTabPremiumAria: "Premium",
      starsGridAria: "Star packages",
      premiumGridAria: "Premium packages",
      cardStars100Aria: "100 stars package",
      cardStars500Aria: "500 stars package",
      cardStars1000Aria: "1000 stars package",
      cardPremium100Aria: "Premium package 3 months",
      cardPremium500Aria: "Premium package 6 months",
      cardPremium1000Aria: "Premium package 12 months",
      premiumDuration3: "3 mo.",
      premiumDuration6: "6 mo.",
      premiumDuration12: "12 mo.",
      badgeValue: "Best value",
      badgePopular: "Popular",
      usernameFloatingLabel: "Enter Telegram @username",
      selfFillButton: "Me",
      amountFloatingLabel: "Enter amount (50 - 10000)",
      tonAmountFloatingLabel: "Enter TON amount",
      promoFloatingLabel: "Enter promo code",
      paymentMethodLabel: "Payment method",
      paymentMethodText: "SBP #1 (PALLY)",
      orderButton: "Place order",
      tonOrderButton: "Send TON",
      orderNote:
        "By purchasing Telegram Stars or Telegram Premium, you confirm your agreement with our Terms and confirm that you are not a resident of the USA.",
      profilePageAria: "Profile page",
      profileCloseAria: "Close profile",
      profileFallbackName: "Telegram user",
      profileLabelUsername: "Telegram username",
      profileLabelTelegramId: "Telegram ID",
      profileHistoryTitle: "Purchase history",
      profileHistoryEmpty: "No purchases yet",
      profileAdminButton: "Admin panel",
      profileAdminButtonHide: "Hide admin panel",
      profileAdminPanelTitle: "Star rates",
      profileAdminSaveButton: "Save rates",
    },
  };

  const storageKey = "starslix_lang";

  const setText = (selector, value) => {
    const element = document.querySelector(selector);
    if (element) {
      element.textContent = value;
    }
  };

  const setAttr = (selector, attr, value) => {
    const element = document.querySelector(selector);
    if (element) {
      element.setAttribute(attr, value);
    }
  };

  const setHeroSecondLines = (dict) => {
    const mobileLine2 = document.querySelector(
      ".hero-title-layout-mobile .hero-title-line:nth-child(2)"
    );
    if (mobileLine2) {
      mobileLine2.innerHTML = `${dict.heroMobileLine2Prefix}<span class="hero-title-accent">${dict.heroStars}</span>`;
    }

    const wideLine2 = document.querySelector(
      ".hero-title-layout-wide .hero-title-line:nth-child(2)"
    );
    if (wideLine2) {
      wideLine2.innerHTML = `<span class="hero-title-accent">${dict.heroStars}</span> ${dict.heroWideLine2Suffix}`;
    }
  };

  const applyLanguage = (lang) => {
    const dict = translations[lang] || translations.ru;

    document.documentElement.lang = lang;
    setAttr("#pageLoader", "aria-label", dict.loaderAria);
    setAttr("#userAvatarToggle", "aria-label", dict.profileButtonAria);

    setText("#telegramBotButton", dict.telegramButton);
    setAttr("#languageToggleButton", "aria-label", dict.languageButton);
    setText("#languageToggleValue", dict.languageShort);
    setText("#languageOptionRu", dict.languageOptionRu);
    setText("#languageOptionEn", dict.languageOptionEn);

    languageOptions.forEach((option) => {
      option.classList.toggle("is-active", option.dataset.lang === lang);
    });

    setText(".hero-title-layout-mobile .hero-title-line:nth-child(1)", dict.heroMobileLine1);
    setHeroSecondLines(dict);
    setText(".hero-title-layout-mobile .hero-title-line:nth-child(3)", dict.heroMobileLine3);
    setText(".hero-title-layout-wide .hero-title-line:nth-child(1)", dict.heroWideLine1);

    setText(".hero-subtitle-line:nth-child(1)", dict.heroSubtitleLine1);
    setText(".hero-subtitle-line:nth-child(2)", dict.heroSubtitleLine2);
    setAttr("#topPill", "aria-label", dict.topPillAria);
    setText("#topPillText", dict.topPillText);
    setAttr("#appDock", "aria-label", dict.dockAria);
    setText("#dockLabelHome", dict.dockHome);
    setText('[data-dock-label-tint="home"]', dict.dockHome);
    setText("#dockLabelLeaders", dict.dockLeaders);
    setText('[data-dock-label-tint="leaders"]', dict.dockLeaders);
    setText("#dockLabelContest", dict.dockContest);
    setText('[data-dock-label-tint="contest"]', dict.dockContest);
    setText("#dockLabelProfile", dict.dockProfile);
    setText('[data-dock-label-tint="profile"]', dict.dockProfile);
    setAttr("#leadersPage", "aria-label", dict.leadersPageAria);
    setText("#leadersTitle", dict.leadersTitle);
    setText("#leadersText", dict.leadersText);
    setAttr("#contestPage", "aria-label", dict.contestPageAria);
    setText("#contestTitle", dict.contestTitle);
    setText("#contestText", dict.contestText);

    setAttr(".main-glass-panel", "aria-label", dict.mainPanelAria);
    setAttr(".panel-tabs", "aria-label", dict.panelTabsAria);
    setAttr('.panel-tab[data-panel="stars"]', "aria-label", dict.panelTabStarsAria);
    setAttr('.panel-tab[data-panel="premium"]', "aria-label", dict.panelTabPremiumAria);

    setAttr(".offer-grid-stars", "aria-label", dict.starsGridAria);
    setAttr(".offer-grid-premium", "aria-label", dict.premiumGridAria);
    setAttr(".offer-grid-stars .offer-card:nth-child(1)", "aria-label", dict.cardStars100Aria);
    setAttr(".offer-grid-stars .offer-card:nth-child(2)", "aria-label", dict.cardStars500Aria);
    setAttr(".offer-grid-stars .offer-card:nth-child(3)", "aria-label", dict.cardStars1000Aria);
    setAttr(".offer-grid-premium .offer-card:nth-child(1)", "aria-label", dict.cardPremium100Aria);
    setAttr(".offer-grid-premium .offer-card:nth-child(2)", "aria-label", dict.cardPremium500Aria);
    setAttr(".offer-grid-premium .offer-card:nth-child(3)", "aria-label", dict.cardPremium1000Aria);
    setText(".offer-grid-premium .offer-card:nth-child(1) .offer-card-amount", dict.premiumDuration3);
    setText(".offer-grid-premium .offer-card:nth-child(2) .offer-card-amount", dict.premiumDuration6);
    setText(".offer-grid-premium .offer-card:nth-child(3) .offer-card-amount", dict.premiumDuration12);

    document.querySelectorAll(".offer-card--value .offer-card-badge").forEach((badge) => {
      badge.textContent = dict.badgeValue;
    });
    document.querySelectorAll(".offer-card--popular .offer-card-badge").forEach((badge) => {
      badge.textContent = dict.badgePopular;
    });

    setText(
      ".form-stack--stars .form-field:nth-child(1) .form-floating-label",
      dict.usernameFloatingLabel
    );
    setText(
      ".form-stack--stars .form-field:nth-child(2) .form-floating-label",
      dict.amountFloatingLabel
    );
    setText(
      ".form-stack--stars .form-field:nth-child(3) .form-floating-label",
      dict.promoFloatingLabel
    );
    setText(
      ".form-stack--stars .form-field:nth-child(4) .form-field-label",
      dict.paymentMethodLabel
    );
    setText(
      ".form-stack--premium .form-field:nth-child(1) .form-floating-label",
      dict.usernameFloatingLabel
    );
    setText(
      ".form-stack--premium .form-field:nth-child(2) .form-floating-label",
      dict.promoFloatingLabel
    );
    setText(
      ".form-stack--premium .form-field:nth-child(3) .form-field-label",
      dict.paymentMethodLabel
    );
    setText(
      ".form-stack--ton .form-field:nth-child(1) .form-floating-label",
      dict.usernameFloatingLabel
    );
    setText(
      ".form-stack--ton .form-field:nth-child(2) .form-floating-label",
      dict.tonAmountFloatingLabel
    );
    setText(
      ".form-stack--ton .form-field:nth-child(3) .form-field-label",
      dict.paymentMethodLabel
    );

    setAttr("#profilePage", "aria-label", dict.profilePageAria);
    setAttr("#profileCloseButton", "aria-label", dict.profileCloseAria);
    setText("#profileLabelUsername", dict.profileLabelUsername);
    setText("#profileLabelId", dict.profileLabelTelegramId);
    setText("#profileHistoryTitle", dict.profileHistoryTitle);
    setText("#profileHistoryEmpty", dict.profileHistoryEmpty);
    setAttr("#profileAdminButton", "data-text-open", dict.profileAdminButton);
    setAttr("#profileAdminButton", "data-text-hide", dict.profileAdminButtonHide);
    if (document.getElementById("profileAdminPanel")?.hidden === false) {
      setText("#profileAdminButton", dict.profileAdminButtonHide);
    } else {
      setText("#profileAdminButton", dict.profileAdminButton);
    }
    setText("#profileAdminPanelTitle", dict.profileAdminPanelTitle);
    setText("#profileAdminSaveButton", dict.profileAdminSaveButton);
    document.querySelectorAll(".form-self-fill-button").forEach((node) => {
      node.textContent = dict.selfFillButton;
    });

    const profileFullname = document.getElementById("profileFullname");
    if (profileFullname?.dataset.empty === "true") {
      profileFullname.textContent = dict.profileFallbackName;
    }

    document.querySelectorAll(".form-static-text").forEach((node) => {
      node.textContent = dict.paymentMethodText;
    });
    document
      .querySelectorAll(".form-stack--stars .order-submit-button, .form-stack--premium .order-submit-button")
      .forEach((node) => {
      node.textContent = dict.orderButton;
    });
    setText(".form-stack--ton .order-submit-button", dict.tonOrderButton);
    document.querySelectorAll(".order-note").forEach((node) => {
      node.textContent = dict.orderNote;
    });

    window.dispatchEvent(new CustomEvent("starslix:dock-labels-updated"));
  };

  const getSavedLanguage = () => {
    try {
      const savedLanguage = window.localStorage.getItem(storageKey);
      return savedLanguage === "en" || savedLanguage === "ru" ? savedLanguage : null;
    } catch {
      return null;
    }
  };

  const saveLanguage = (lang) => {
    try {
      window.localStorage.setItem(storageKey, lang);
    } catch {
      // ignore write errors
    }
  };

  let currentLanguage = getSavedLanguage() || "ru";
  applyLanguage(currentLanguage);

  let isDropdownOpen = false;

  const setDropdownState = (open) => {
    isDropdownOpen = open;
    languageDropdown.classList.toggle("is-open", open);
    languageDropdown.setAttribute("aria-hidden", open ? "false" : "true");
    languageToggleButton.setAttribute("aria-expanded", open ? "true" : "false");
  };

  languageToggleButton.addEventListener("click", (event) => {
    event.stopPropagation();
    setDropdownState(!isDropdownOpen);
  });

  languageOptions.forEach((option) => {
    option.addEventListener("click", (event) => {
      event.stopPropagation();
      const nextLanguage = option.dataset.lang;
      if (nextLanguage !== "ru" && nextLanguage !== "en") {
        return;
      }

      currentLanguage = nextLanguage;
      applyLanguage(currentLanguage);
      saveLanguage(currentLanguage);
      setDropdownState(false);
    });
  });

  document.addEventListener("pointerdown", (event) => {
    if (!isDropdownOpen) {
      return;
    }

    const target = event.target;
    if (!(target instanceof Element)) {
      return;
    }

    if (target.closest("#headerLanguageSwitch")) {
      return;
    }

    setDropdownState(false);
  });

  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape" && isDropdownOpen) {
      setDropdownState(false);
    }
  });
})();

(() => {
  const block = (event) => {
    event.preventDefault();
  };

  document.addEventListener("copy", block);
  document.addEventListener("cut", block);
  document.addEventListener("contextmenu", block);
  document.addEventListener("dragstart", block);

  document.addEventListener("selectstart", (event) => {
    const target = event.target;
    if (target instanceof Element && target.closest("input, textarea")) {
      return;
    }

    event.preventDefault();
  });

  document.addEventListener("keydown", (event) => {
    if (!(event.ctrlKey || event.metaKey)) {
      return;
    }

    const key = event.key.toLowerCase();
    if (key === "c" || key === "x") {
      event.preventDefault();
    }
  });
})();

