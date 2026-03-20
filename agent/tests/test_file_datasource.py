from pathlib import Path

import pytest

from file_datasource import FileDatasource


def write_csv(path: Path, header: str, rows: list[str]) -> None:
    path.write_text(header + "\n" + "\n".join(rows) + "\n", encoding="utf-8")


@pytest.fixture
def sample_csvs(tmp_path: Path):
    acc = tmp_path / "accelerometer.csv"
    gps = tmp_path / "gps.csv"
    park = tmp_path / "parking.csv"
    bus = tmp_path / "bus.csv"

    write_csv(acc, "x,y,z", ["1,2,3", "4,5,6"])
    write_csv(gps, "longitude,latitude", ["30.1,50.1", "30.2,50.2"])
    write_csv(park, "empty_count,longitude,latitude", ["7,30.3,50.3", "8,30.4,50.4"])
    write_csv(bus, "bus_id,longitude,latitude,occupancy_rate", ["101,30.5,50.5,0.25", "102,30.6,50.6,0.75"])

    return acc, gps, park, bus


def make_datasource(sample_csvs, user_id: int = 99) -> FileDatasource:
    acc, gps, park, bus = sample_csvs
    return FileDatasource(str(acc), str(gps), str(park), str(bus), user_id)


def test_start_reading_validates_required_headers(tmp_path: Path):
    acc = tmp_path / "accelerometer.csv"
    gps = tmp_path / "gps.csv"
    park = tmp_path / "parking.csv"
    bus = tmp_path / "bus.csv"

    write_csv(acc, "x,y", ["1,2"])
    write_csv(gps, "longitude,latitude", ["30.1,50.1"])
    write_csv(park, "empty_count,longitude,latitude", ["7,30.3,50.3"])
    write_csv(bus, "bus_id,longitude,latitude,occupancy_rate", ["101,30.5,50.5,0.25"])

    datasource = FileDatasource(str(acc), str(gps), str(park), str(bus), user_id=1)

    with pytest.raises(ValueError):
        datasource.startReading()


def test_read_returns_three_non_empty_collections(sample_csvs):
    datasource = make_datasource(sample_csvs)
    datasource.startReading()

    agg, park, buses = datasource.read()

    assert 5 <= len(agg) <= 7
    assert 5 <= len(park) <= 7
    assert 5 <= len(buses) <= 7
    assert all(item.user_id == 99 for item in agg)

    datasource.stopReading()


def test_parking_reader_cycles_back_to_file_start(sample_csvs):
    datasource = make_datasource(sample_csvs)
    datasource.startReading()

    park1 = datasource._read_single_park()
    park2 = datasource._read_single_park()
    park3 = datasource._read_single_park()

    assert park1.empty_count == 7
    assert park2.empty_count == 8
    assert park3.empty_count == 7

    datasource.stopReading()
