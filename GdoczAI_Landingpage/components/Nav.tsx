"use client";

import React, { useState, useEffect } from "react";
import { Button, Drawer } from "antd";
import { MenuOutlined, GithubOutlined } from "@ant-design/icons";
import { useGithubStars } from "./provider/GithubStarsProvider";

export default function Nav() {
  const [open, setOpen] = useState(false);
  const [mounted, setMounted] = useState(false);
  const stars = useGithubStars();
  const appUrl = process.env.NEXT_PUBLIC_APP_URL;

  useEffect(() => {
    setMounted(true);
  }, []);

  const showDrawer = () => setOpen(true);
  const onClose = () => setOpen(false);

  const handleScroll = (e: React.MouseEvent<HTMLAnchorElement>, id: string) => {
    e.preventDefault();
    onClose();
    const element = document.getElementById(id);
    if (element) {
      element.scrollIntoView({ behavior: "smooth", block: "start" });
    }
  };

  const NavLinks = () => (
    <>
      <li><a href="#how-it-works" onClick={(e) => handleScroll(e, "how-it-works")}>How it works</a></li>
      <li><a href="#security" onClick={(e) => handleScroll(e, "security")}>Security</a></li>
      <li><a href="#editions" onClick={(e) => handleScroll(e, "editions")}>Cloud &amp; Self-Hosted</a></li>
      <li><a href="#use-cases" onClick={(e) => handleScroll(e, "use-cases")}>Use cases</a></li>
      {/* <li><a href="https://gramosoft.tech/gdoczai/pricing">Pricing</a></li> */}
      <li><a href="https://gramosoft.tech/blog">Blog</a></li>
    </>
  );

  return (
    <nav className="site-nav" aria-label="Main navigation">
      <div className="wrap nav-in d-flex align-items-center justify-content-between">
        <a className="logo" href="https://gramosoft.tech/gdoczai">
          {/* <span className="mark">G</span>Gdocz<em>AI</em> */}
          <img src={`${appUrl}/assets/icons/logo.svg`} alt="logo" />
        </a>
        <ul className="nav-links d-none d-lg-flex m-0 list-unstyled gap-4">
          <NavLinks />
        </ul>

        <div className="nav-cta gs-nav-cta d-flex align-items-center gap-3">

          <div className="d-none d-lg-flex align-items-center gap-3">
            <a
              href="https://github.com/GramosoftAI/GdoczAI"
              target="_blank"
              rel="noopener noreferrer"
              className="gs-github-star d-inline-flex align-items-center me-2"
            >
              <GithubOutlined style={{ fontSize: "16px" }} />
              <span className="ms-1">Star</span>
              <span className="gs-github-star-divider"></span>
              <span className="gs-github-star-count">{stars}</span>
            </a>
            <a className="signin" href={`${appUrl}/auth/sign_in`}>Sign in</a>
            {/* <Button className="btn btn-primary" href="https://app.gramosoft.tech/signup" type="primary">
              Sign up free
            </Button> */}
          </div>

          <button
            className="d-lg-none btn btn-outline d-inline-flex align-items-center"
            onClick={showDrawer}
            style={{
              border: "1.5px solid var(--line)",
              borderRadius: "20px",
              height: "32px",
              fontWeight: 600,
              fontSize: "13px",
              padding: "0 14px",
              display: "flex",
              alignItems: "center",
              gap: "4px",
              color: "var(--muted)",
              background: "#fff"
            }}
          >
            <MenuOutlined style={{ fontSize: '14px' }} />
            <span>Menu</span>
          </button>
        </div>

        {mounted && (
          <Drawer
            title={
              <span className="logo">
                {/* <span className="mark">G</span>Gdocz<em>AI</em> */}
                <img src={`${appUrl}/assets/icons/logo.svg`} alt="logo" />
              </span>
            }
            placement="right"
            onClose={onClose}
            open={open}
            width={280}
          >
            <ul className="mobile-nav-links list-unstyled d-flex flex-column gap-3">
              <NavLinks />
              <hr />
              <li>
                <a
                  href="https://github.com/GramosoftAI/GdoczAI"
                  target="_blank"
                  rel="noopener noreferrer"
                  className="gs-github-star w-100 justify-content-center d-inline-flex align-items-center py-2"
                  onClick={onClose}
                  style={{ height: 40, borderRadius: 20 }}
                >
                  <GithubOutlined style={{ fontSize: "16px" }} />
                  <span className="ms-2">Star on GitHub</span>
                  <span className="gs-github-star-divider mx-2"></span>
                  <span className="gs-github-star-count">{stars}</span>
                </a>
              </li>
              <li>
                <a className="signin d-block text-center mb-1 text-decoration-none fw-semibold" href={`${appUrl}/auth/sign_in`} onClick={onClose}>
                  Sign in
                </a>
              </li>
              <li>
                <Button className="w-100 btn-primary py-2 d-flex align-items-center justify-content-center text-white" href={`${appUrl}/auth/sign_in`} type="primary" onClick={onClose} style={{ borderRadius: 10, height: 40 }}>
                  Sign up free
                </Button>
              </li>
            </ul>
          </Drawer>
        )}

      </div>
    </nav>
  );
}
