"use client";

import { useMemo, useState } from "react";
import { Slider } from "antd";

function formatINR(n: number) {
  return n.toLocaleString("en-IN");
}

export default function RoiCalculator() {
  const [docs, setDocs] = useState(2000);
  const [mins, setMins] = useState(4);
  const [rate, setRate] = useState(400);

  const { save, hours } = useMemo(() => {
    const hrs = (docs * mins) / 60;
    return { save: Math.round(hrs * rate), hours: Math.round(hrs) };
  }, [docs, mins, rate]);

  return (
    <section aria-label="ROI calculator">
      <div className="roi">
        <div className="center">
          <span className="kicker">ROI calculator</span>
          <h2>See how much GdoczAI saves you</h2>
          <p className="lead">Move the sliders to match your workflow. Savings update in real time.</p>
        </div>
        <div className="row gy-5 align-items-center mt-4">
          <div className="col-12 col-lg-6">
            <label className="d-flex justify-content-between align-items-center mb-1">
              <span>Documents per month</span>
              <output className="fw-bold" style={{ color: "#fff" }}>{formatINR(docs)}</output>
            </label>
            <Slider
              min={100}
              max={100000}
              step={100}
              value={docs}
              onChange={setDocs}
              tooltip={{ formatter: (v) => formatINR(v ?? 0) }}
              aria-label="Documents per month"
            />

            <label className="d-flex justify-content-between align-items-center mb-1 mt-4">
              <span>Minutes to key in one document manually</span>
              <output className="fw-bold" style={{ color: "#fff" }}>{mins}</output>
            </label>
            <Slider
              min={1}
              max={15}
              step={1}
              value={mins}
              onChange={setMins}
              aria-label="Minutes per document"
            />

            <label className="d-flex justify-content-between align-items-center mb-1 mt-4">
              <span>Hourly cost of that work (₹)</span>
              <output className="fw-bold" style={{ color: "#fff" }}>{formatINR(rate)}</output>
            </label>
            <Slider
              min={100}
              max={2000}
              step={50}
              value={rate}
              onChange={setRate}
              tooltip={{ formatter: (v) => formatINR(v ?? 0) }}
              aria-label="Hourly labor cost in rupees"
            />
          </div>
          <div className="col-12 col-lg-6">
            <div className="roi-big w-100">
              <div className="amt">₹{formatINR(save)}</div>
              <div className="lbl">estimated manual-entry cost recovered per month</div>
              <div className="sub">≈ {formatINR(hours)} hours of re-keying eliminated</div>
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}

