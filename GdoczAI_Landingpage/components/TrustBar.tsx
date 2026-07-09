export default function TrustBar() {
  const logos = ["Lion Air Group", "Batik Air", "Thai Lion Air", "Sundaram Motors", "VST Motors"];
  return (
    // EDIT ME: confirm client name/logo usage permissions before publishing
    <div className="trust">
      <div className="wrap">
        <p>Powering document workflows for teams across aviation, automotive &amp; finance</p>
        <div className="logo-row">
          {logos.map((name) => (
            <span key={name}>{name}</span>
          ))}
        </div>
      </div>
    </div>
  );
}
