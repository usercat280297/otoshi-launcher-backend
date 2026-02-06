from sqlalchemy.orm import Session

from .models import (
    Achievement,
    BadgeDefinition,
    Bundle,
    DlcItem,
    Game,
    TradingCardDefinition,
    User,
    WorkshopItem,
    WorkshopVersion,
)

sample_video = {
    "url": "https://interactive-examples.mdn.mozilla.net/media/cc0-videos/flower.mp4",
    "thumbnail": "https://images.unsplash.com/photo-1500530855697-b586d89ba3ee?auto=format&fit=crop&w=900&q=80",
}

shared_requirements = {
    "minimum": {
        "os": "Windows 10 64-bit",
        "processor": "Intel i5-8400 / Ryzen 5 2600",
        "memory": "12 GB RAM",
        "graphics": "GTX 1060 / RX 580",
        "storage": "80 GB SSD",
    },
    "recommended": {
        "os": "Windows 11 64-bit",
        "processor": "Intel i7-12700K / Ryzen 7 5800X",
        "memory": "16 GB RAM",
        "graphics": "RTX 3070 / RX 6800",
        "storage": "80 GB NVMe SSD",
    },
}

SAMPLE_GAMES = [
    {
        "slug": "aurora-shift",
        "title": "Aurora Shift",
        "tagline": "Pilot a living starship across fractured galaxies.",
        "short_description": "A cinematic space odyssey with tactical combat and modular ships.",
        "description": "Aurora Shift is a cinematic space odyssey blending tactical combat with narrative exploration.",
        "studio": "Arclight Studios",
        "developer": "Arclight Studios",
        "publisher": "PulseWorks",
        "release_date": "2025-08-12",
        "genres": ["Action", "RPG", "Space Opera"],
        "tags": ["Space", "Narrative", "Tactical"],
        "platforms": ["windows"],
        "price": 39.99,
        "discount_percent": 30,
        "rating": 4.6,
        "header_image": "https://images.unsplash.com/photo-1451187580459-43490279c0fa?auto=format&fit=crop&w=900&q=80",
        "hero_image": "https://images.unsplash.com/photo-1446776811953-b23d57bd21aa?auto=format&fit=crop&w=1400&q=80",
        "background_image": "https://images.unsplash.com/photo-1462331940025-496dfbfc7564?auto=format&fit=crop&w=1600&q=80",
        "screenshots": [
            "https://images.unsplash.com/photo-1451187580459-43490279c0fa?auto=format&fit=crop&w=1200&q=80",
            "https://images.unsplash.com/photo-1462331940025-496dfbfc7564?auto=format&fit=crop&w=1200&q=80",
            "https://images.unsplash.com/photo-1454789548928-9efd52dc4031?auto=format&fit=crop&w=1200&q=80",
        ],
        "videos": [sample_video],
        "system_requirements": shared_requirements,
    },
    {
        "slug": "emberfall",
        "title": "Emberfall",
        "tagline": "Forge cities from volcanic relics.",
        "short_description": "Heat-economy strategy builder on a volcanic archipelago.",
        "description": "Emberfall is a strategy builder set in a volcanic archipelago.",
        "studio": "Gravemark",
        "developer": "Gravemark",
        "publisher": "ForgeLine",
        "release_date": "2024-11-03",
        "genres": ["Strategy", "Simulation"],
        "tags": ["City Builder", "Volcanic"],
        "platforms": ["windows"],
        "price": 29.99,
        "discount_percent": 0,
        "rating": 4.2,
        "header_image": "https://images.unsplash.com/photo-1500530855697-b586d89ba3ee?auto=format&fit=crop&w=900&q=80",
        "hero_image": "https://images.unsplash.com/photo-1500530855697-b586d89ba3ee?auto=format&fit=crop&w=1400&q=80",
        "background_image": "https://images.unsplash.com/photo-1501785888041-af3ef285b470?auto=format&fit=crop&w=1600&q=80",
        "screenshots": [
            "https://images.unsplash.com/photo-1501785888041-af3ef285b470?auto=format&fit=crop&w=1200&q=80",
            "https://images.unsplash.com/photo-1500530855697-b586d89ba3ee?auto=format&fit=crop&w=1200&q=80",
            "https://images.unsplash.com/photo-1476611338391-6f395a0ebc86?auto=format&fit=crop&w=1200&q=80",
        ],
        "videos": [sample_video],
        "system_requirements": shared_requirements,
    },
    {
        "slug": "reefline",
        "title": "Reefline",
        "tagline": "Dive into neon oceans and decode lost biomes.",
        "short_description": "Co-op deep-sea exploration with drone mapping and puzzles.",
        "description": "Reefline blends deep-sea exploration with cooperative puzzle hunting.",
        "studio": "Midnight Current",
        "developer": "Midnight Current",
        "publisher": "Abyssal Point",
        "release_date": "2025-03-22",
        "genres": ["Adventure", "Co-op"],
        "tags": ["Ocean", "Puzzle"],
        "platforms": ["windows"],
        "price": 24.99,
        "discount_percent": 15,
        "rating": 4.8,
        "header_image": "https://images.unsplash.com/photo-1507525428034-b723cf961d3e?auto=format&fit=crop&w=900&q=80",
        "hero_image": "https://images.unsplash.com/photo-1507525428034-b723cf961d3e?auto=format&fit=crop&w=1400&q=80",
        "background_image": "https://images.unsplash.com/photo-1469474968028-56623f02e42e?auto=format&fit=crop&w=1600&q=80",
        "screenshots": [
            "https://images.unsplash.com/photo-1469474968028-56623f02e42e?auto=format&fit=crop&w=1200&q=80",
            "https://images.unsplash.com/photo-1507525428034-b723cf961d3e?auto=format&fit=crop&w=1200&q=80",
            "https://images.unsplash.com/photo-1500375592092-40eb2168fd21?auto=format&fit=crop&w=1200&q=80",
        ],
        "videos": [sample_video],
        "system_requirements": shared_requirements,
    },
    {
        "slug": "sable-circuit",
        "title": "Sable Circuit",
        "tagline": "Synthwave racer with tactical boosts and sabotage.",
        "short_description": "Neon racer with EMP traps, drift boosts, and sabotage.",
        "description": "Sable Circuit is a high-speed combat racer.",
        "studio": "Chrome Hollow",
        "developer": "Chrome Hollow",
        "publisher": "VoltLine",
        "release_date": "2025-05-18",
        "genres": ["Racing", "Arcade"],
        "tags": ["Racing", "Synthwave"],
        "platforms": ["windows"],
        "price": 19.99,
        "discount_percent": 40,
        "rating": 4.1,
        "header_image": "https://images.unsplash.com/photo-1500534623283-312aade485b7?auto=format&fit=crop&w=900&q=80",
        "hero_image": "https://images.unsplash.com/photo-1489515217757-5fd1be406fef?auto=format&fit=crop&w=1400&q=80",
        "background_image": "https://images.unsplash.com/photo-1493238792000-8113da705763?auto=format&fit=crop&w=1600&q=80",
        "screenshots": [
            "https://images.unsplash.com/photo-1493238792000-8113da705763?auto=format&fit=crop&w=1200&q=80",
            "https://images.unsplash.com/photo-1489515217757-5fd1be406fef?auto=format&fit=crop&w=1200&q=80",
            "https://images.unsplash.com/photo-1500534623283-312aade485b7?auto=format&fit=crop&w=1200&q=80",
        ],
        "videos": [sample_video],
        "system_requirements": shared_requirements,
    },
    {
        "slug": "veilborne",
        "title": "Veilborne",
        "tagline": "A tactical RPG where every spell distorts time.",
        "short_description": "Time-bending tactical RPG with layered timelines.",
        "description": "Veilborne is a tactical RPG with a real-time timewarp layer.",
        "studio": "Northwind Atelier",
        "developer": "Northwind Atelier",
        "publisher": "Chronicle",
        "release_date": "2024-09-09",
        "genres": ["RPG", "Tactics"],
        "tags": ["Tactics", "Time"],
        "platforms": ["windows"],
        "price": 49.99,
        "discount_percent": 10,
        "rating": 4.7,
        "header_image": "https://images.unsplash.com/photo-1471879832106-c7ab9e0cee23?auto=format&fit=crop&w=900&q=80",
        "hero_image": "https://images.unsplash.com/photo-1441974231531-c6227db76b6e?auto=format&fit=crop&w=1400&q=80",
        "background_image": "https://images.unsplash.com/photo-1469474968028-56623f02e42e?auto=format&fit=crop&w=1600&q=80",
        "screenshots": [
            "https://images.unsplash.com/photo-1471879832106-c7ab9e0cee23?auto=format&fit=crop&w=1200&q=80",
            "https://images.unsplash.com/photo-1441974231531-c6227db76b6e?auto=format&fit=crop&w=1200&q=80",
            "https://images.unsplash.com/photo-1500530855697-b586d89ba3ee?auto=format&fit=crop&w=1200&q=80",
        ],
        "videos": [sample_video],
        "system_requirements": shared_requirements,
    },
    {
        "slug": "radiant-frontier",
        "title": "Radiant Frontier",
        "tagline": "Survive the first light on an alien world.",
        "short_description": "Survival co-op across a tidally locked alien world.",
        "description": "Radiant Frontier is a survival co-op set on a tidally locked planet.",
        "studio": "Horizon Forge",
        "developer": "Horizon Forge",
        "publisher": "LumenWorks",
        "release_date": "2025-12-02",
        "genres": ["Survival", "Co-op", "Sci-Fi"],
        "tags": ["Survival", "Sci-Fi"],
        "platforms": ["windows"],
        "price": 34.99,
        "discount_percent": 0,
        "rating": 4.3,
        "header_image": "https://images.unsplash.com/photo-1496307653780-42ee777d4833?auto=format&fit=crop&w=900&q=80",
        "hero_image": "https://images.unsplash.com/photo-1496307653780-42ee777d4833?auto=format&fit=crop&w=1400&q=80",
        "background_image": "https://images.unsplash.com/photo-1454789548928-9efd52dc4031?auto=format&fit=crop&w=1600&q=80",
        "screenshots": [
            "https://images.unsplash.com/photo-1496307653780-42ee777d4833?auto=format&fit=crop&w=1200&q=80",
            "https://images.unsplash.com/photo-1454789548928-9efd52dc4031?auto=format&fit=crop&w=1200&q=80",
            "https://images.unsplash.com/photo-1446776811953-b23d57bd21aa?auto=format&fit=crop&w=1200&q=80",
        ],
        "videos": [sample_video],
        "system_requirements": shared_requirements,
    }
]


def seed_games(db: Session) -> None:
    existing_games = {game.slug: game for game in db.query(Game).all()}
    for payload in SAMPLE_GAMES:
        existing = existing_games.get(payload["slug"])
        if not existing:
            db.add(Game(**payload))
            continue
        for key, value in payload.items():
            current = getattr(existing, key, None)
            if current in (None, "", [], {}):
                setattr(existing, key, value)
    db.commit()

    seed_achievements(db)
    seed_phase10(db)


def seed_achievements(db: Session) -> None:
    existing = {(item.game_id, item.key) for item in db.query(Achievement).all()}
    games = db.query(Game).all()
    for game in games:
        achievements = [
            {
                "game_id": game.id,
                "key": "first_launch",
                "title": "First Launch",
                "description": "Boot the game for the first time.",
                "points": 10,
            },
            {
                "game_id": game.id,
                "key": "hour_played",
                "title": "In the Zone",
                "description": "Play for one hour.",
                "points": 25,
            },
        ]
        for achievement in achievements:
            if (achievement["game_id"], achievement["key"]) in existing:
                continue
            db.add(Achievement(**achievement))
    db.commit()


def seed_phase10(db: Session) -> None:
    games = db.query(Game).all()
    if not games:
        return

    if not db.query(Bundle).first():
        bundle = Bundle(
            slug="starter-pack",
            title="Starter Pack",
            description="A curated bundle of launch titles.",
            price=49.99,
            discount_percent=20,
            game_ids=[game.id for game in games[:3]],
        )
        db.add(bundle)

    if not db.query(DlcItem).first():
        base_game = games[0]
        dlc = DlcItem(
            base_game_id=base_game.id,
            title=f"{base_game.title} - Season Pass",
            description="Bonus missions, soundtrack, and early updates.",
            price=14.99,
            is_season_pass=True,
            release_date=base_game.release_date,
        )
        db.add(dlc)

    creator = db.query(User).first()
    if creator and not db.query(WorkshopItem).first():
        base_game = games[0]
        item = WorkshopItem(
            game_id=base_game.id,
            creator_id=creator.id,
            title="High Contrast UI Pack",
            description="Improves readability with higher contrast HUD elements.",
            item_type="mod",
            visibility="public",
            tags=["ui", "accessibility"],
        )
        db.add(item)
        db.flush()
        db.add(
            WorkshopVersion(
                workshop_item_id=item.id,
                version="1.0.0",
                changelog="Initial release",
                file_size=0,
                download_url="/workshop/items/{}/versions/latest/download".format(item.id),
            )
        )

    if not db.query(TradingCardDefinition).first():
        for game in games[:3]:
            db.add_all(
                [
                    TradingCardDefinition(game_id=game.id, card_name="Nova", rarity="common"),
                    TradingCardDefinition(game_id=game.id, card_name="Echo", rarity="uncommon"),
                    TradingCardDefinition(game_id=game.id, card_name="Vortex", rarity="rare"),
                ]
            )

    if not db.query(BadgeDefinition).first():
        for game in games[:3]:
            db.add(
                BadgeDefinition(
                    game_id=game.id,
                    badge_name="Founders Badge",
                    level=1,
                    required_cards=["Nova", "Echo", "Vortex"],
                    xp_reward=120,
                )
            )

    db.commit()
