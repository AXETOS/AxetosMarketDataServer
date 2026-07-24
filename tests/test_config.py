from axetos_market_data.config import ConfigurationStore, ProviderConfig


def test_provider_configuration_round_trip(tmp_path):
    store = ConfigurationStore(tmp_path / "providers.json")
    store.upsert(ProviderConfig("Mock.Local", "Local Mock", symbols=["EUR/USD"]))
    values = store.read_all()
    assert len(values) == 1
    assert values[0].provider_key == "Mock.Local"
    assert values[0].normalized_symbols() == ["EUR/USD"]
